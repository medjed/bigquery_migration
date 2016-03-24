require "bigquery_migration/version"
require "bigquery_migration/error"
require "bigquery_migration/schema"
require "bigquery_migration/logger"
require "bigquery_migration/bigquery_wrapper"

class BigqueryMigration
  def self.logger
    @logger ||= Logger.new(STDOUT)
  end

  def self.logger=(logger)
    @logger = logger
  end

  def initialize(*args)
    @wrapper = BigqueryWrapper.new(*args)
  end

  # Delegate to BigqueryWrapper instance
  BigqueryWrapper.instance_methods(false).each do |name|
    next if method_defined?(name)
    class_eval <<-"EOS", __FILE__, __LINE__ + 1
      def #{name}(*args, &block)
        @wrapper.#{name}(*args, &block)
      end
    EOS
  end
end
