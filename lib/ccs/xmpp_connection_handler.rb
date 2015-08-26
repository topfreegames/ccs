module CCS
  class XMPPConnectionHandler
    include Celluloid 
    include Celluloid::Logger

    attr_reader :connection_count, :sender_id, :api_key

    def initialize(params={})      
      return if params[:connection_count] <= 0
      
      @handler_name = params[:handler_name]
      @connection_count = params[:connection_count]
      @sender_id = params[:sender_id]
      @api_key = params[:api_key]

      Actor[@handler_name] = Actor.current

      CCS.debug "Create #{@connection_count} connections"
      (@connection_count).times do
        add_connection
      end
      CCS.debug "Initialized connection handler for #{@sender_id}"
    end

    def redis
      @redis ||= RedisHelper.connection(:celluloid)
    end
    
    def queue_size
      number_of_messages = 0
      (1..1000).each do |n|
        number_of_messages += redis.llen(xmpp_connection_queue(n)) || 0
      end
      number_of_messages +=  redis.llen(xmpp_queue) || 0
      return number_of_messages
    end

    def xmpp_queue
      @xmpp_queue ||= "#{@sender_id}:#{XMPP_QUEUE}"
    end

    def xmpp_connection_queue(connection_id)
      "#{@sender_id}:#{XMPP_QUEUE}:#{connection_id}"
    end

    def terminate_child(id)
      CCS.debug "Terminate drained actor (#{xmpp_connection_queue(id)})"
      Actor[xmpp_connection_queue(id)].terminate 
      requeue(id)
      add_connection
    end

    def add_connection
      connection_n = next_connection_number 
      if connection_n.nil?
        CCS.info "no available connection to send ccs messages!"
        return
      end

      if !@supervisor
        @supervisor = XMPPConnection.supervise({id: connection_n, handler: @handler_name, sender_id: @sender_id, api_key: @api_key})
      else
        @supervisor.add(XMPPConnection, args: [{id: connection_n, handler: @handler_name, sender_id: @sender_id, api_key: @api_key}])
      end

      CCS.debug "Connection created for CCS! (connection_id=#{connection_n})"
    end

    def next_connection_number
      (1..1000).each do |n|
        exists, size = redis.multi do |m|
          m.exists(xmpp_connection_queue(n))
          m.lpush(xmpp_connection_queue(n), "#{CONN_PLACEHOLDER}")
        end
        return n if(!exists && size == 1)
      end
      nil
    end

    private

    def requeue(id)
      redis.merge_and_delete(xmpp_connection_queue(id), xmpp_queue)
    end
  end
end