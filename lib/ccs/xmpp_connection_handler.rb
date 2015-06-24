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

      (@connection_count).times do
        add_connection
      end
      debug "Initialized connection handler for #{@sender_id}"
    end

    def queue_size
      number_of_messages = 0
      connections = RedisHelper.smembers(connections_set_key)
      connections.each do |n|
        number_of_messages += RedisHelper.llen(xmpp_connection_queue(n)) || 0
      end
      number_of_messages +=  RedisHelper.llen(xmpp_queue) || 0
      return number_of_messages
    end

    def xmpp_queue
      @xmpp_queue ||= "#{sender_id}:#{XMPP_QUEUE}"
    end

    def xmpp_connection_queue(connection_id)
      "#{sender_id}:#{XMPP_QUEUE}:#{connection_id}"
    end

    def connections_set_key
      @connections_set_key ||= "#{sender_id}:#{CONNECTIONS}"
    end

    def terminate_child(id)
      debug "Terminate drained actor (#{xmpp_connection_queue(id)})"
      Actor[xmpp_connection_queue(id)].terminate
      requeue(id)
    end

    def add_connection
      if !@supervisor
        @supervisor = XMPPConnection.supervise(id: next_connection_number, handler: @handler_name, sender_id: @sender_id, api_key: @api_key)
      else
        XMPPConnection.supervise(id: next_connection_number, handler: @handler_name, sender_id: @sender_id, api_key: @api_key)
      end
    end

    def remove_connection(id)
      RedisHelper.srem(connections_set_key, id)
    end

    def next_connection_number
      (1..100).each do |n|
        return n if RedisHelper.sadd(connections_set_key, n)
      end
      nil
    end

    private

    def requeue(id)
      RedisHelper.merge_and_delete(xmpp_connection_queue(id), xmpp_queue)
      RedisHelper.srem(connections_set_key, id)
      RedisHelper.del(connections_set_key) if RedisHelper.smembers(connections_set_key).empty?
    end
  end
end
