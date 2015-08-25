module CCS
  class Configuration
    attr_accessor :host, :redis_url
    attr_reader :drain_timeout, :queue_ttl, :queue_ttl_interval, :port, :dry_run, :default_time_to_live, :default_delay_while_idle, :default_delivery_receipt_requested

    def initialize
      @host               = 'gcm.googleapis.com'
      @port               = 5235
      @redis_url          = 'redis://localhost:6379'
      @drain_timeout      = 5
      @default_time_to_live = 0
      @default_delay_while_idle = false
      @default_delivery_receipt_requested = false
      @queue_ttl_interval = 60
      @queue_ttl          = 600
      @drain_timeout      = 10
      @dry_run            = false
    end

    def port=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      fail 'must be between 0 and 65535' unless value.between?(0, 65_535)
      @port = value
    end

    def dry_run=(value)
      @dry_run = value ? true : false
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
    def queue_ttl=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      @queue_ttl = value
    end

    def queue_ttl_interval=(value)
      fail 'must be a fixnum' unless value.class == Fixnum
      @queue_ttl_interval = value
    end

    private

    def validate_redis
      RedisHelper.ping
    rescue Redis::CannotConnectError => e
      raise e.to_s
    end
  end
end
