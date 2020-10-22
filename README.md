# logstash-filter-age
Filter to calculate age of an event based on when it was received by Logstash.
It can optionally determine the limit by which an event is considered expired
and perform the calculation. This helps keep magic numbers out your logstash filter.
