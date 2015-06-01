module CCS
  class XMPPConnectionHandler
    include Celluloid 
    include Celluloid::Logger

    attr_reader :connection_count, :sender_id, :api_key

    def initialize(params={})      
      return if params[:connection_count] <= 0

      @connection_count = params[:connection_count]
      @sender_id = params[:sender_id]
      @api_key = params[:api_key]

      requeue
      @supervisor = XMPPConnection.supervise(id: next_connection_number, handler: self, sender_id: @sender_id, api_key: @api_key)
      (@connection_count - 1).times do
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
      @xmpp_connection_queue ||= "#{sender_id}:#{XMPP_QUEUE}:#{connection_id}"
    end

    def connections_set_key
      @connections_set_key ||= "#{sender_id}:#{CONNECTIONS}"
    end

    def drain
      add_connection
    end

    def add_connection
      @supervisor.add(XMPPConnection, args: [{id: next_connection_number, handler: self, sender_id: sender_id, api_key: api_key}])
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

    def requeue
      prev = RedisHelper.smembers(connections_set_key)
      prev.each do |n|
        RedisHelper.merge_and_delete(xmpp_connection_queue(n), xmpp_queue)
      end
      RedisHelper.del(connections_set_key)
    end
  end
end
