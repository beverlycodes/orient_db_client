# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "orient_db_client/version"

Gem::Specification.new do |s|
  s.name        = "orient_db_client"
  s.version     = OrientDbClient::VERSION
  s.authors     = ["Ryan Fields"]
  s.email       = ["ryan.fields@twoleftbeats.com"]
  s.homepage    = ""
  s.summary     = %q{TODO: Write a gem summary}
  s.description = %q{TODO: Write a gem description}

  s.rubyforge_project = "orient_db_client"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_development_dependency "minitest"
  s.add_development_dependency "mocha"
  s.add_development_dependency "rake"
  # s.add_runtime_dependency "rest-client"
end
