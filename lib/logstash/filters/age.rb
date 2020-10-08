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

  # Define the elasticsearch url to the limit service
  config :url, :default => 'https://elasticsearch.main.top.elliemae.io/_cluster/settings?filter_path=persistent.cluster.metadata.optimizer.sleep_min_seconds', :validate => :string

  config :limit_path, :default => 'persistent.cluster.metadata.optimizer.sleep_min_seconds', :validate => :string

  # The max_age_secs is the default number of seconds beyond which the expired_target will be set to true (when the limit service url not found or no result))
  config :max_age_secs, :default => 259200, :validate => :number

  # The expired_target field is boolean when the event timestamp  is older than max_age_secs
  config :expired_target, :default => '[@metadata][expired]', :validate => :string

  # The interval between calls to the limit service given by the url
  config :interval, :default => "60s", :validate => :string

  # user and password come from the http client mixin
  # the password needs to be dereferenced using @password.value

  public
  def register
    @split_limit_path = limit_path.split(".")
    @scheduler = Rufus::Scheduler.new

    request_limit()

    @scheduler.every @interval do
      request_limit()
    end
  end

# See: https://www.elastic.co/guide/en/logstash/current/filter-new-plugin.html
# See: https://github.com/logstash-plugins/logstash-filter-elasticsearch/blob/master/lib/logstash/filters/elasticsearch.rb
# See: https://github.com/logstash-plugins/logstash-filter-http/blob/master/lib/logstash/filters/http.rb
# See: https://www.rubydoc.info/gems/logstash-mixin-http_client/4.0.2/LogStash/PluginMixins/HttpClient
# See: https://github.com/logstash-plugins/logstash-input-http_poller
# See: https://www.elastic.co/guide/en/logstash/current/event-api.html
# See: https://discuss.elastic.co/t/how-to-parse-json-values-from-http-poller-into-event-fields/136597
# See: https://www.elastic.co/guide/en/logstash/current/multiple-input-output-plugins.html
#
# curl -u 'obs_ro:Password!234' https://elasticsearch.main.dev.top.rd.elliemae.io/_cluster/settings?filter_path=**.metadata
# curl -u 'obs_ro:Password!234' https://elasticsearch.main.top.elliemae.io/_cluster/settings?filter_path=**.metadata
# curl -u 'obs_ro:Password!234' https://elasticsearch.main.top.elliemae.io/_cluster/settings?filter_path=persistent.cluster.metadata.optimizer.sleep_min_seconds
#{
#  "persistent": {
#    "cluster": {
#      "metadata": {
#        "optimizer": {
#          "index_creation_enable": "True",
#          "shard_rebalance_enable": "True",
#          "sleep_max_seconds": "30",
#          "shard_rebalance_ratio": "93.5",
#          "shard_rebalance_max_run_time_minutes": "480",
#          "ok_unassigned_shards_when_red": "20",
#          "sleep_min_seconds": "10",
#          "start_time_utc": "06:00:05",
#          "index_creation_max_run_time_minutes": "480",
#          "status": ""
#        },
#        "unsafe-bootstrap": "true"
#      }
#    }
#  }
#}
#
# curl -u 'obs_ro:Password!234' https://elasticsearch.main.top.elliemae.io/_cluster/settings?filter_path=**.metadata.optimizer.sleep_min_seconds
#{"persistent":{"cluster":{"metadata":{"optimizer":{"sleep_min_seconds":"10"}}}}

  public
  def filter(event)

    delta = Time.now.to_f - event.timestamp.to_f
    event.set(@target, delta)

    if delta > @max_age_secs
    	event.set(@expired_target, true)
    else
    	event.set(@expired_target, false)
    end

    event.set("ageresponse", @ageresponse)

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end

  private
  def request_limit
    begin

      options = {auth: {user: @user, password: @password.value}}
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
                    :url => @url, :code => code,
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
      #parsed = LogStash::Json.load(body)
      parsed = LogStash::Json.load(body).to_hash

      @split_limit_path.each do |field|
          break if !parsed
          parsed = parsed.dig(field)
      end

      if parsed
          @ageresponse = parsed
      else
          @ageresponse = @max_age_secs
      end

#@ageresponse = parsed.dig("persistent", "cluster", "metadata", "optimizer", "sleep_min_seconds")
#@ageresponse = parsed["persistent"]["cluster"]["metadata"]["optimizer"]["sleep_min_seconds"]
#    "ageresponse" => {
#        "persistent" => {
#            "cluster" => {
#                "metadata" => {
#                    "optimizer" => {
#                        "sleep_min_seconds" => "10"
#                    }
#                }
#            }
#        }
    rescue => e
        @logger.warn('JSON parsing error', :message => e.message, :body => body)
    end
  end
end
