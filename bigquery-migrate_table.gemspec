# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bigquery/migrate_table/version'

Gem::Specification.new do |spec|
  spec.name          = "bigquery-migrate_table"
  spec.version       = Bigquery::MigrateTable::VERSION
  spec.authors       = ["Naotoshi Seo"]
  spec.email         = ["sonots@gmail.com"]

  spec.summary       = %q{Migrate BigQuery tables}
  spec.description   = %q{Migrate BigQuery tables.}
  spec.homepage      = "https://github.com/sonots/bigquery-migrate_table"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "google-api-client"
  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "pry-byebug"
  spec.add_development_dependency "test-unit"
  spec.add_development_dependency "test-unit-rr"
  spec.add_development_dependency "test-unit-power_assert"
end
