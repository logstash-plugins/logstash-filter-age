# encoding: utf-8

require 'logstash/filters/base'
require 'logstash/namespace'
require 'logstash/plugin_mixins/http_client'
require 'logstash/json'
require 'jruby/synchronized'
require 'rufus-scheduler'

class LogStash::Filters::Age < LogStash::Filters::Base
  include LogStash::PluginMixins::HttpClient
  include JRuby::Synchronized

  config_name 'age'

  # Define the target field for the event age, in seconds.
  config :target, :default => '[@metadata][age]', :validate => :string

  # There are two optional features that can be enabled:
  #
  # 1. Have age perform the calculation:
  #   if current time - event time > max_age_secs set the boolean value in
  #   the expired_target field. 
  #   This feature is enabled by setting:
  #   a) a non zero value in the max_age_secs setting.
  #
  # 2. Have age determine age_limit to be used in the calculation:
  #   if current time - event time > age_limit max_age_secs
  #     set the boolean value in the expired_target field. 
  #   This feature is enabled by setting:
  #   a) max_age_secs must be non zero
  #   b) url must be defined (the url to a limit service returning a json to a 
  #      leaf level age limit value)
  #   c) user and password to the url service
  #      TODO: support other auth approaces
  #   d) limit_path defined as the lmit service json response body path to
  #      the age_limit value
  #   e) interval is the frequency between url requests to get the latest
  #      age limit.
  #   f) age_limit_target is the field to store the discovered age limit
  #
  # The max_age_secs is the default number of seconds beyond which the 
  # expired_target will be set to true (when the limit service url not found or 
  # there is no result)
  config :max_age_secs, :default => 259200, :validate => :number

  # Define the elasticsearch url to the limit service
  config :url, :default => '', :validate => :string

  # The response to the limit url will be a json with a nested structure the ends
  # in a numeric age limit . The limit_path is a dot delimited representation
  # of the nested json returned in the limit service response body.
  config :limit_path,
    :default => 'persistent.cluster.metadata.logstash.filter.age.limit_secs', 
    :validate => :string

  # The expired_target field is true when the event is older than age_limit
  config :expired_target, 
    :default => '[@metadata][expired]',
    :validate => :string

  # The age_limit_target is the name of the field whose value is the number of 
  # seconds actually used in the calculated result stored in expired_target
  # When url is defined and the limit is found, then this is the discovered value
  config :age_limit_target, 
    :default => '[@metadata][age_limit]',
    :validate => :string

  # The interval between calls to the limit service given by the url
  config :interval, :default => "60s", :validate => :string

  # user and password (and other options) come from the http client mixin
  # Note that the password needs to be dereferenced using @password.value

  public
  def register
    if url != ''
      @logger.debug('age filter is configured to use a limit service')
      @split_limit_path = limit_path.split(".")
      @scheduler = Rufus::Scheduler.new

      request_limit()

      @scheduler.every @interval do
        request_limit()
      end
    else
      @logger.debug('age filter is not configured to use a limit service')
      @age_limit = @max_age_secs.to_f
    end
  end

  public
  def filter(event)

    delta = Time.now.to_f - event.timestamp.to_f
    event.set(@target, delta)

    if delta > @age_limit
      event.set(@expired_target, true)
    else
      event.set(@expired_target, false)
    end

    event.set(@age_limit_target, @age_limit)

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end

  private
  def request_limit
    begin

      options = {auth: {user: @user, password: @password.value},
        request_timeout: @request_timeout, socket_timeout: @socket_timeout,
        connect_timeout: @connect_timeout, automatic_retries: @automatic_retries}

      code, response_headers, response_body = request_http(@url, options)

    rescue => e
      client_error = e
    end

    if client_error
      @logger.error('error during HTTP request',
        :url => @url,
        :client_error => client_error.message)

    elsif !code.between?(200, 299)
      @logger.error('error during HTTP request',
        :url => @url,
        :code => code,
        :response => response_body)
    else
      process_response(response_body)
    end
  end

  def request_http(url, options = {})
    @logger.info('age making request_http with arguments', :url => url)
    response = client.http("get", url, options)
    [response.code, response.headers, response.body]
  end

  def process_response(body)
    begin
      parsed = LogStash::Json.load(body).to_hash

      @split_limit_path.each do |field|
        break if !parsed
        parsed = parsed.dig(field)
      end

      if parsed
        @age_limit = parsed.to_f
        if @age_limit <= 0
          @age_limit = @max_age_secs.to_f
          @logger.info('age response parsed non numeric',
            :age_limit => @age_limit, :parsed => parsed)
        else
          @logger.info('age response parsed numeric',
            :age_limit => @age_limit, :parsed => parsed)
        end
      else
          @age_limit = @max_age_secs.to_f
          @logger.info('age response parsed false (using max_age_secs)',
            :age_limit => @age_limit, :parsed => parsed, :max_age_secs => @max_age_secs)
      end

    rescue => e
      @logger.warn('JSON parsing error', :message => e.message, :body => body)
    end
  end
end
