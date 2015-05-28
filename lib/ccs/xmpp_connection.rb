module CCS
  class XMPPConnection < XMPPSimple::Api
    include Celluloid::IO
    include Celluloid::Logger

    attr_reader :id, :sender_id, :api_key

    def initialize(id:, handler:, sender_id:, api_key:)
      @id = id
      @state = :disconnected
      @draining = false
      @handler = handler
      @sender_id = sender_id
      @api_key = api_key

      reset
      XMPPSimple.logger = CCS.logger
      @xmpp_client = XMPPSimple::Client.new(Actor.current,
                                            sender_id,
                                            api_key,
                                            CCS.configuration.host,
                                            CCS.configuration.port).connect
    end

    def upstream_queue
      @upstream_queue ||= "#{sender_id}:#{UPSTREAM_QUEUE}"
    end

    def error_queue
      @error_queue ||= "#{sender_id}:#{XMPP_ERROR_QUEUE}"
    end

    def receipt_queue
      @receipt_queue ||= "#{sender_id}:#{RECEIPT_QUEUE}"
    end  

    def xmpp_queue
      @xmpp_queue ||= "#{sender_id}:#{XMPP_QUEUE}"
    end

    def xmpp_connection_queue
      @xmpp_connection_queue ||= "#{sender_id}:#{XMPP_QUEUE}:#{@id}"
    end

    def sender_loop
      redis = RedisHelper.connection(:celluloid)
      while @state == :connected && !@draining
        next unless @semaphore.take
        debug "waiting in ccs connection"
        msg_str = redis.brpoplpush(xmpp_queue, xmpp_connection_queue)
        debug "got message in ccs connection"
        msg = MultiJson.load(msg_str)
        send_stanza(msg)
        @send_messages[msg['message_id']] = msg_str
      end
    end

    def ack(msg)
      CCS.debug("Ack: #{msg}")
      content = {}
      content['to']           = msg['from']
      content['message_id']   = msg['message_id']
      content['message_type'] = 'ack'
      send_stanza(content)
    end

    def send_stanza(content)
      msg  = '<message>'
      msg += '<gcm xmlns="google:mobile:data">'
      msg += MultiJson.dump(content)
      msg += '</gcm>'
      msg += '</message>'
      CCS.debug "Write: #{msg}"
      @xmpp_client.write_data(msg)
    end

    def drain
      @handler.drain
      @draining = true
      @semaphore.interrupt
    end

    def reset
      @send_messages = {}
      @semaphore = Semaphore.new(MAX_MESSAGES)

      RedisHelper.merge_and_delete(xmpp_connection_queue, xmpp_queue)
    end

    # simple xmpp handler method
    def reconnecting
      CCS.debug('Reconnecting')
      @state = :reconnecting
      reset
    end

    # simple xmpp handler method
    def connected
      CCS.debug('Connected')
      @state = :connected
      async.sender_loop
    end

    # simple xmpp handler method
    def disconnected
      CCS.debug('Disconnected')
      @state = :disconnected
      @semaphore.interrupt
    end

    # simple xmpp handler method
    def message(node)
      xml = Ox.parse(node)
      plain_content = xml.locate('gcm/^Text').first
      content = MultiJson.load(plain_content)
      if xml['type'] == 'error'
        # Should not happen
      end

      return if content.nil?
      CCS.debug("Type: #{content['message_type']}")
      case content['message_type']
      when nil
        CCS.debug('Received upstream message')
        # upstream
        RedisHelper.rpush(upstream_queue, MultiJson.dump(content))
        ack(content)
      when 'ack'
        handle_ack(content)
      when 'nack'
        handle_nack(content)
      when 'receipt'
        handle_receipt(content)
      when 'control'
        handle_control(content)
      else
        CCS.info("Received unknown message type: #{content['message_type']}")
      end
    end

    private

    def handle_receipt(content)
      CCS.debug("Delivery receipt received for: #{content['message_id']}")
      RedisHelper.rpush(receipt_queue, MultiJson.dump(content))
      ack(content)
    end

    def handle_ack(content)
      msg = @send_messages.delete(content['message_id'])
      if msg.nil?
        CCS.info("Received ack for unknown message: #{content['message_id']}")
      else
        msg.delete('message_id')
        if RedisHelper.lrem(xmpp_connection_queue, -1, msg) < 1
          CCS.debug("NOT FOUND: #{MultiJson.dump(msg)}")
        end
        @semaphore.release
      end
    end

    def handle_nack(content)
      msg = @send_messages.delete(content['message_id'])
      if msg.nil?
        CCS.info("Received nack for unknown message: #{content['message_id']}")
      else
        msg.delete('message_id')
        RedisHelper.lrem(xmpp_connection_queue, -1, msg)
        RedisHelper.rpush(error_queue, MultiJson.dump("message" => msg,  "error" => content['error']))
      end
    end

    # the connection will be closed, drain it
    def handle_control(content)
      case content['control_type']
      when 'CONNECTION_DRAINING'
        drain
      else
        CCS.info("Received unknown control type: #{content['control_type']}")
      end
    end
  end
end
