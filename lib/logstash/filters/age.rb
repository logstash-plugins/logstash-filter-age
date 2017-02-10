# encoding: utf-8

require 'logstash/filters/base'
require 'logstash/namespace'

# A simple filter for calculating the age of an event.
#
# This filter calculates the age of an event by subtracting the event timestamp
# from the current timestamp. This allows you to drop Logstash events that are
# older than some threshold.
#
# [source,ruby]
# filter {
#   age {}
#
#   if [@metadata][age] > 86400 {
#     drop {}
#   }
# }
#
class LogStash::Filters::Age < LogStash::Filters::Base

  config_name 'age'

  # Define the target field for the event age, in seconds.
  config :target, :default => '[@metadata][age]', :validate => :string

  public
  def register
    # Nothing to do here
  end

  public
  def filter(event)
    event.set(@target, Time.now.to_f - event.timestamp.to_f)
    filter_matched(event)
  end
end
