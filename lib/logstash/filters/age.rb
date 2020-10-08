# encoding: utf-8

require 'logstash/filters/base'
require 'logstash/namespace'
# New below
require 'logstash/plugin_mixins/http_client'
require 'logstash/json'

class LogStash::Filters::Age < LogStash::Filters::Base
  include LogStash::PluginMixins::HttpClient

  config_name 'age'

  # Define the target field for the event age, in seconds.
  config :target, :default => '[@metadata][age]', :validate => :string

  # Define the elasticsearch url to the cluster settings for max_age_secs 
  config :url, :default => 'https://elasticsearch.main.top.elliemae.io/_cluster/settings?filter_path=persistent.cluster.metadata.optimizer.sleep_min_seconds', :validate => :string

  # The max_age_secs is the number of seconds beyond which the expired_target will be set to true (default: 259200)
  config :max_age_secs,:default => 0.01, :validate => :number

  # The expired_target field is boolean when the event timestamp  is older than max_age_secs
  config :expired_target, :default => '[@metadata][expired]', :validate => :string

  # user and password come from the http client mixin
  # the password needs to be dereferenced using @password.value

  public
  def register
    begin

#[2020-10-07T23:48:36,365][WARN ][logstash.filters.age     ][main] JSON parsing error {:message=>"undefined local variable or method `event' for #<LogStash::Filters::Age:0x572572e7>", :body=>"{\"persistent\":{\"cluster\":{\"metadata\":{\"optimizer\":{\"sleep_min_seconds\":\"10\"}}}}}"}

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
  def request_http(url, options = {})
    @logger.debug('age making request_http with arguments', :url => url)
    response = client.http("get", url, options)
    [response.code, response.headers, response.body]
  end

  def process_response(body)
    begin
      parsed = LogStash::Json.load(body)
      @ageresponse = parsed
    rescue => e
        @logger.warn('JSON parsing error', :message => e.message, :body => body)
    end
  end
end
