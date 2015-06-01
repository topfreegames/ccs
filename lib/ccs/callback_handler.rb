module CCS
  class CallbackHandler
    include Celluloid

    attr_reader :callback, :sender_id

    def initialize(params={})
      @sender_id = params[:sender_id]
      @redis = RedisHelper.connection(:celluloid)
      @handler_name = params[:handler_name]
      @callback = {}
      async.run
    end

    def upstream_queue
      @upstream_queue ||= "#{sender_id}:#{UPSTREAM_QUEUE}"
    end

    def error_queue
      @error_queue ||= "#{sender_id}:#{XMPP_ERROR_QUEUE}"
    end

    def receipt_queue
      @receipt_queue ||= "#{sender_id}:#{RECEIPT_QUEUE}"
    end  

    def handler
      Celluloid::Actor[@handler_name]
    end

    def run
      CCS.logger.info "starting ccs callback handler for #{sender_id}"
      loop do
        begin
          list, value = @redis.blpop(upstream_queue, error_queue, receipt_queue, 0)
          msg = MultiJson.load(value)
          case list
          when upstream_queue
            handler.on_upstream(msg)
          when error_queue
            handler.on_error(msg)
          when receipt_queue
            handler.on_receipt(msg)
          end
        rescue => e
          CCS.error e.inspect
        end
      end
    end
  end
end
