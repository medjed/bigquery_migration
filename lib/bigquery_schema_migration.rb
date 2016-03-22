require "bigquery_schema_migration/version"
require "bigquery_schema_migration/error"
require "bigquery_schema_migration/schema"
require "bigquery_schema_migration/logger"

class BigquerySchemaMigration
  def self.logger
    @logger ||= Logger.new(STDOUT)
  end

  def self.logger=(logger)
    @logger = logger
  end
end
