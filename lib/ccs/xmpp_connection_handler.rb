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
      debug "Initialized connection handler for #{sender_id}"
    end

    def queue_size
      number_of_messages = 0
      (1..1000).each do |n|
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

    def terminate_child(id)
      debug "Terminate drained actor (#{xmpp_connection_queue(id)})"
      Actor[xmpp_connection_queue(id)].terminate
      requeue(id)
    end

    def add_connection
      connection_n = next_connection_number 
      return if connection_n.nil?

      if !@supervisor
        @supervisor = XMPPConnection.supervise(args: [{id: connection_n, handler: @handler_name, sender_id: sender_id, api_key: @api_key}])
      else
        @supervisor.add(XMPPConnection, args: [{id: connection_n, handler: @handler_name, sender_id: sender_id, api_key: @api_key}])
      end
    end

    def next_connection_number
      (1..1000).each do |n|
        return n if RedisHelper.exists(xmpp_connection_queue(n)) == 0
      end
    end

    private

    def requeue(id)
      RedisHelper.merge_and_delete(xmpp_connection_queue(id), xmpp_queue)
    end
  end
end
