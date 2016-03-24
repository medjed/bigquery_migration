require "bigquery_migration/version"
require "bigquery_migration/error"
require "bigquery_migration/schema"
require "bigquery_migration/logger"
require "bigquery_migration/bigquery_wrapper"
require 'forwardable'

class BigqueryMigration
  extend Forwardable

  def self.logger
    @logger ||= Logger.new(STDOUT)
  end

  def self.logger=(logger)
    @logger = logger
  end

  def initialize(*args)
    @wrapper = BigqueryWrapper.new(*args)
  end

  def_delegators :@wrapper,
    :client,
    :existing_columns,
    :get_dataset,
    :insert_dataset,
    :create_dataset, # alias
    :get_table,
    :insert_table,
    :create_table, # alias
    :delete_table,
    :drop_table, # alias
    :list_tables,
    :purge_tables,
    :insert_all_table_data,
    :list_table_data,
    :patch_table,
    :add_column, # alias
    :copy_table,
    :insert_select,
    :drop_column,
    :migrate_table
end
