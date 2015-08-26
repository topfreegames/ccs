require 'celluloid/redis'

module CCS
  module RedisHelper
    
    module PatchedRedis 
      def merge_and_delete(source, destination)
        lrem(source, 0, "#{CONN_PLACEHOLDER}")
        messages = lrange(source, 0, -1)
        pipelined do
          rpush(destination, messages) unless messages.empty?
          del(source)
        end
      end
    end
    
    Redis.send(:include, PatchedRedis)

    module_function

    def connection(driver = :celluloid)
      Redis.new(url: CCS.configuration.redis_url, driver: driver)
    end
  end
end
