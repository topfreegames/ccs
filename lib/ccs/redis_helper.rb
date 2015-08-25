module CCS
  module RedisHelper
    module_function

    def srem(key, value)
      redis.srem(key, value)
    end

    def lrem(key, pos, value)
      redis.lrem(key, pos, value)
    end

    def llen(key)
      redis.llen(key)
    end

    def sadd(key, value)
      redis.sadd(key, value)
    end

    def smembers(key)
      redis.smembers(key)
    end

    def exists(key)
      redis.exists(key)
    end

    def lrange(key, a, b)
      redis.lrange(key, a, b)
    end

    def rpush(key, value)
      redis.rpush(key, value)
    end

    def lpush(key, value)
      redis.lpush(key, value)
    end

    def del(key)
      redis.del(key)
    end

    def ping
      redis.ping
    end

    def ttl(key)
      redis.ttl(key)
    end

    def expire(key, seconds)
      redis.expire(key, seconds)
    end

    def merge_and_delete(source, destination)
      messages = redis.lrange(source, 0, -1)
      redis.pipelined do
        redis.rpush(destination, messages) unless messages.empty?
        redis.del(source)
      end
    end

    def connection(driver = :celluloid)
      Redis.new(url: CCS.configuration.redis_url,
                driver: driver)
    end

    def redis
      @redis ||= connection
    end
  end
end
