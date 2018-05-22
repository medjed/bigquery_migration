# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bigquery_migration/version'

Gem::Specification.new do |spec|
  spec.name          = "bigquery_migration"
  spec.version       = BigqueryMigration::VERSION
  spec.authors       = ["Naotoshi Seo", "kysnm", "potato2003"]
  spec.email         = ["sonots@gmail.com", "tokyoincidents.g@gmail.com", "potato2003@gmail.com"]

  spec.summary       = %q{Migrate BigQuery table schema}
  spec.description   = %q{Migrate BigQuery table schema.}
  spec.homepage      = "https://github.com/sonots/bigquery_migration"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "google-api-client"
  spec.add_dependency "tzinfo"
  spec.add_dependency "thor"
  spec.add_dependency "inifile"

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "pry-byebug"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "test-unit-rr"
  spec.add_development_dependency "test-unit-power_assert"
end
