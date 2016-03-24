class BigqueryMigration
  class Error < StandardError; end
  class ConfigError < Error; end
  class JobTimeoutError < Error; end
  class NotFoundError < Error; end
end
