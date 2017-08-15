Gem::Specification.new do |s|
  s.name          = 'logstash-filter-age'
  s.version         = '1.0.2'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'A Logstash filter for calculating the age of an event.'
  s.description   = 'This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program'
  s.homepage      = 'https://github.com/joshuaspence/logstash-filter-json'
  s.authors       = ['Joshua Spence']
  s.email         = 'josh@joshuaspence.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir["lib/**/*","spec/**/*","*.gemspec","*.md","CONTRIBUTORS","Gemfile","LICENSE","NOTICE.TXT", "vendor/jar-dependencies/**/*.jar", "vendor/jar-dependencies/**/*.rb", "VERSION", "docs/**/*"]
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a Logstash plugin
  s.metadata = { 'logstash_plugin' => 'true', 'logstash_group' => 'filter' }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_development_dependency 'logstash-devutils'
end
