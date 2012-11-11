# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "orient_db_client/version"

Gem::Specification.new do |s|
  s.name        = "orient_db_client"
  s.version     = OrientDbClient::VERSION
  s.authors     = ["Ryan Fields"]
  s.email       = ["ryan.fields@twoleftbeats.com"]
  s.homepage    = ""
  s.summary     = %q{Network Binary Protocol client for OrientDB Server}
  s.description = %q{This gem uses the OrientDB Network Binary Protocol to provide connectivity to an OrientDB Server}

  s.rubyforge_project = "orient_db_client"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency "bindata", "~> 1.4.0"

  s.add_development_dependency "minitest", "~> 3.1.0"
  s.add_development_dependency "mocha", "~> 0.12.0"
  s.add_development_dependency "rake"
  s.add_development_dependency "pry"
end
