require 'multi_json'
require 'json'
require 'nokogiri'
require 'xmpp_simple'
require 'celluloid'
require 'celluloid/redis'

require 'ccs/version'
require 'ccs/redis_helper'
require 'ccs/configuration'
require 'ccs/notification'
require 'ccs/user_notification'
require 'ccs/semaphore'
require 'ccs/callback_handler'
require 'ccs/xmpp_connection_handler'
require 'ccs/xmpp_connection_monitor'
require 'ccs/xmpp_connection'
require 'ccs/http_worker'

module CCS

  XMPP_ERROR_QUEUE   = 'ccs_xmpp_error'
  XMPP_QUEUE         = 'ccs_xmpp_sending'
  UPSTREAM_QUEUE     = 'ccs_upstream'
  RECEIPT_QUEUE      = 'ccs_receipt'
  CONN_PLACEHOLDER   = 'connection_placeholder'

  ACK_COUNTER        = 'ack_counter'
  NACK_COUNTER       = 'nack_counter'
  SENT_COUNTER       = 'sent_counter'
  BACKOFF_COUNTER    = 'backoff_counter'

  CONNECTIONS        = 'ccs_connections'
  MAX_MESSAGES       = 100
  MAX_BACKOFF        = 5

  module_function

  ## Main functions

  ## Start connection for single project
  def start(params={})
    api_key = params[:api_key]
    sender_id = params[:sender_id]
    connection_count = params[:connection_count] || 1
    configuration.valid?
    XMPPConnectionHandler.new(api_key: api_key, sender_id: sender_id, connection_count: connection_count)
    callback_handler = CallbackHandler.new
    yield callback_handler if block_given?
    callback_handler
  end

  ## Configuration
  def configuration
    @configuration ||= Configuration.new
  end

  def configure
    yield(configuration) if block_given?
  end

  def reset_configuration
    @configuration = Configuration.new
  end

  ## Logging

  def logger=(value)
    @logger = value
  end

  def logger
    @logger ||= Logger.new($stdout).tap do |logger|
      logger.level = Logger::ERROR
      logger.formatter = proc do |severity, datetime, progname, msg|
        "#{severity} :: #{datetime.strftime('%d-%m-%Y :: %H:%M:%S')} :: #{progname} :: #{msg}\n"
      end
    end
  end

  def debug(message)
    logger.debug(message.inspect)
  end

  def info(message)
    logger.info(message.inspect)
  end

  def error(message)
    logger.error(message.inspect)
  end

  def mutex
    @mutex ||= Mutex.new.tap { @last_id = 0 }
  end

  def next_id
    mutex.synchronize do
      @last_id += 1
      return format('%010x', @last_id)
    end
  end

    def xmpp_queue(sender_id)
      @xmpp_queue ||= "#{sender_id}:#{RECEIPT_QUEUE}"
    end

  ## CCS Api
  def send(sender_id, to, data = {}, options = {})
    msg = Notification.new(to, data, options)
    id = next_id
    msg.message_id = id
    return send_notification(sender_id, msg)
  end

  ## GCM Api
  def create(sender_id, api_key, registration_ids, notification_key_name)
    fail 'name cannot be nil' if notification_key_name.nil?
    fail 'registration_ids must be an array' unless registration_ids.is_a? Array
    notification_key('create', sender_id, api_key, registration_ids, notification_key_name)
  end

  def add(sender_id, api_key, registration_ids, notification_key, notification_key_name = nil)
    fail 'key cannot be nil' if notification_key.nil?
    fail 'registration_ids must be an array' unless registration_ids.is_a? Array
    notification_key('add', sender_id, api_key, registration_ids, notification_key_name, notification_key)
  end

  def remove(sender_id, api_key, registration_ids, notification_key, notification_key_name = nil)
    fail 'key cannot be nil' if notification_key.nil?
    fail 'registration_ids must be an array' unless registration_ids.is_a? Array
    notification_key('remove', sender_id, api_key, registration_ids, notification_key_name, notification_key)
  end

  def notification_key(operation, sender_id, api_key, registration_ids, notification_key_name = nil, notification_key = nil)
    msg = UserNotification.new(operation, registration_ids, notification_key_name, notification_key)
    HTTPWorker.new.query(sender_id, api_key, msg)
  end
end
