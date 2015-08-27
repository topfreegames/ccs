require 'timers'

module CCS
  class XMPPConnectionMonitor
    include Celluloid 
    include Celluloid::Logger

    def initialize(params={})
      @sender_id = params[:sender_id]
      async.run
    end

    def redis
      @redis ||= RedisHelper.connection(:celluloid)
    end

    # If a queue is live for more than the given period it should be drained
    def run
      queue_ttl          = CCS.configuration.queue_ttl
      queue_ttl_interval = CCS.configuration.queue_ttl_interval

      (1..1000).each do |i|
        redis.expire(xmpp_connection_queue(i), queue_ttl)
      end

      timers.every(queue_ttl_interval) do 
        (1..1000).each do |i|
          monitor_queue_ttl(xmpp_connection_queue(i))
        end
      end 
    end

    def monitor_queue_ttl(id, queue_ttl)
      CCS.debug("Renew queue ttl queue=#{id} ttl=#{queue_ttl}")
      redis.expire(id, queue_ttl)
    end

    def xmpp_connection_queue(connection_id)
      "#{@sender_id}:#{XMPP_QUEUE}:#{connection_id}"
    end
  end
end