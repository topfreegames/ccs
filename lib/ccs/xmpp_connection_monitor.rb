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

    def run
      queue_ttl          = CCS.configuration.queue_ttl
      queue_ttl_interval = CCS.configuration.queue_ttl_interval

      (1..1000).each do |i|
        redis.expire(xmpp_connection_queue(i), queue_ttl)
      end

      every(queue_ttl_interval) do 
        (1..1000).each do |i|
          CCS.debug("Renew queue ttl queue=#{i} ttl=#{queue_ttl}")
          redis.expire(xmpp_connection_queue(i), queue_ttl)
        end
      end 
    end

    def xmpp_connection_queue(connection_id)
      "#{@sender_id}:#{XMPP_QUEUE}:#{connection_id}"
    end
  end
end