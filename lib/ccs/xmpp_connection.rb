module CCS
  class XMPPConnection < XMPPSimple::Api
    include Celluloid::IO
    include Celluloid::Logger

    attr_reader :id, :sender_id, :api_key

    def initialize(params={})
      @state = :disconnected
      @draining = false
      @handler = params[:handler]
      @sender_id = params[:sender_id]
      @api_key = params[:api_key]
      @id = params[:id]

      Actor[xmpp_connection_queue] = Actor.current

      reset
      XMPPSimple.logger = CCS.logger
      @xmpp_client = XMPPSimple::Client.new(Actor.current, @sender_id, @api_key, CCS.configuration.host, CCS.configuration.port).connect
      monitor_queue_ttl
    end

    def upstream_queue
      @upstream_queue ||= "#{@sender_id}:#{UPSTREAM_QUEUE}"
    end

    def error_queue
      @error_queue ||= "#{@sender_id}:#{XMPP_ERROR_QUEUE}"
    end

    def receipt_queue
      @receipt_queue ||= "#{@sender_id}:#{RECEIPT_QUEUE}"
    end  

    def xmpp_queue
      @xmpp_queue ||= "#{@sender_id}:#{XMPP_QUEUE}"
    end

    def xmpp_connection_queue
      @xmpp_connection_queue ||= "#{@sender_id}:#{XMPP_QUEUE}:#{@id}"
    end

    # If a queue is live for more than 70 minutes it should be drained (GCM soudld do this before 70 mins)
    def monitor_queue_ttl 
      debug "Monitor queue #{id} each #{CCS.configuration.queue_ttl_interval} seconds. If no key found on redis, drain!"
      queue_ttl          = CCS.configuration.queue_ttl
      queue_ttl_interval = CCS.configuration.queue_ttl_interval

      RedisHelper.expire(id, queue_ttl)
      every(queue_ttl_interval) do
        break if @draining

        curr_ttl = RedisHelper.ttl(id)
        case curr_ttl
          when -1  
            warn "No ttl found for connection #{id}. Defining it as #{queue_ttl} seconds"
            RedisHelper.expire(id, queue_ttl)
          when -2 
            error "Connection #{id} has no active redis key. Drain!"
            drain 
          else
            RedisHelper.expire(id, queue_ttl)
          end
        end
      end
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
      @draining = true
      wait_responses{ Actor[@handler].async.terminate_child(id) }
      Actor[@handler].async.add_connection
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

      xml = Nokogiri::XML(node)
      xml.remove_namespaces!
      plain_content = xml.at_xpath('//message//gcm/text()').text
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

    def wait_responses(limit_s=CCS.configuration.drain_timeout)
      debug "Wait #{limit_s} seconds until releasing #{@handler} (waiting for #{@send_messages.size} messages)"
      every(1) do
        if limit_s <= 0 || @send_messages.empty?
          yield if block_given?
        else
          limit_s -= 1
        end
      end
    end

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
        drain unless @draining
      else
        CCS.info("Received unknown control type: #{content['control_type']}")
      end
    end
  end
end
