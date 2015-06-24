module CCS
  class Configuration
    attr_accessor :host
    attr_accessor :redis_url
    attr_reader :drain_timeout
    attr_reader :port
    attr_reader :default_time_to_live, :default_delay_while_idle, :default_delivery_receipt_requested

    def initialize
      @host             = 'gcm.googleapis.com'
      @port             = 5235
      @redis_url        = 'redis://localhost:6379'
      @drain_timeout    = 5
    end

    def port=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      fail 'must be between 0 and 65535' unless value.between?(0, 65_535)
      @port = value
    end

    def default_time_to_live=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      fail 'must be between 0 and 2419200 seconds' unless value.between?(0, 2_419_200)
      @default_time_to_live = value
    end

    def default_delay_while_idle=(value)
      @default_delay_while_idle = value ? true : false
    end

    def default_delivery_receipt_requested=(value)
      @default_delivery_receipt_requested = value ? true : false
    end

    def valid?
      validate_redis
    end

    def drain_timeout=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      @drain_timeout = value
    end

    private

    def validate_redis
      RedisHelper.ping
    rescue Redis::CannotConnectError => e
      raise e.to_s
    end
  end
end
