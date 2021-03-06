#!/usr/bin/env ruby

require 'test/unit'
require 'test/unit/rr'
require 'pry'
require 'bigquery_migration'

APP_ROOT = File.dirname(__dir__)
TEST_ROOT = File.join(APP_ROOT, 'test')
JSON_KEYFILE = File.join(APP_ROOT, "example/your-project-000.json")

BigqueryMigration.logger = Logger.new(nil)
