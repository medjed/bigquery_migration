require "bigquery_schema_migration/error"
require "bigquery_schema_migration/schema"

module Bigquery
  class MigrateTable
    attr_reader :config

    def initialize(config)
      @config = config
    end

    def self.supported_actions
      %i[
        create_table
        delete_table
        add_column
        drop_column
        migrate_table
        insert
        tabledata_list
      ]
    end

    def create_table
      CreateTable.new(config).run
    end

    def delete_table
      DeleteTable.new(config).run
    end

    def add_column
      AddColumn.new(config).run
    end

    def drop_column
      DropColumn.new(config).run
    end

    def migrate_table
      MigrateTable.new(config).run
    end

    def insert
      Insert.new(config).run
    end

    def tabledata_list
      TabledataList.new(config).run
    end

    class ActionBase
      attr_reader :config

      def initialize(config)
        @config = config
      end

      def dry_run?
        Option[:dry_run]
      end

      def client_options
        {
          'client_id' => config.client_id,
          'project_id' => config.project_id,
          'service_email' => config.service_email,
          'key' => config['key'] || config['private_key'],
          'dataset' => dataset,
          'faraday_option' => {
            'open_timeout' => 300, # sec,
            'timeout' => 600, # sec,
          },
        }
      end

      def client
        @client ||= ::BigQuery::Client.new(client_options)
      end

      def close
        @client.close rescue nil
        @client = nil
      end

      def dataset
        config.dataset || raise(Medjed::Bulk::ConfigError, '[BigQuery::ActionBase] `dataset` is required.')
      end

      def table
        config.table || raise(Medjed::Bulk::ConfigError, '[BigQuery::ActionBase] `table` is required.')
      end

      def max_results
        config.max_results || 99999999
      end

      def job_status_polling_interval
        config.job_status_polling_interval || 5
      end

      def job_status_max_polling_time
        config.job_status_max_polling_time || 3600
      end

      def existing_columns
        if client.tables_formatted.include?(table)
          result = client.describe_table(table)
          result['schema']['fields'].map { |column| Hashie::Mash.new(column) }
        else
          {}
        end
      end
    end

    # action: delete_table
    # type: bigquery
    # config:
    #   <<: *bigquery_source
    #   dataset: (required)
    #   table:   (required)
    class DeleteTable < ActionBase
      def validate_multiple_delete_params!
        config.purge_before || raise(Medjed::Bulk::ConfigError, '[BigQuery::DeleteTable] `purge_before` is required.')
        config.purge_before.is_a?(String) || raise(Medjed::Bulk::ConfigError, '[BigQuery::DeleteTable] `purge_before` must be a string.')
        config.table_prefix || raise(Medjed::Bulk::ConfigError, '[BigQuery::DeleteTable] `table_prefix` is required.')
        config.suffix_format || raise(Medjed::Bulk::ConfigError, '[BigQuery::DeleteTable] `suffix_format` is required.')
      end

      def run
        if config.purge_before
          validate_multiple_delete_params!

          before_tables = list_tables

          delete_tables(before_tables).each do |tbl|
            if dry_run?
              Medjed::Bulk.logger.info  { "(DRY-RUN) delete_table(#{tbl})" }
            else
              Medjed::Bulk.logger.info  { "(EXECUTE) delete_table(#{tbl})" }
              # raises Google::Apis::ClientError if not exists
              client.delete_table(tbl) # no response

              # If you make more than 100 requests per second, throttling might occur.
              # See https://cloud.google.com/bigquery/quota-policy#apirequests
              sleep 1
            end
          end

          after_tables = list_tables
          { result: { delete_tables: (before_tables - after_tables) } }
        else # delete a table
          if dry_run?
            Medjed::Bulk.logger.info  { "(DRY-RUN) delete_table(#{table})" }
          else
            begin
              Medjed::Bulk.logger.info  { "(EXECUTE) delete_table(#{table})" }
              client.delete_table(table)
            rescue Google::Apis::ClientError
              Medjed::Bulk.logger.warn { "(EXECUTE) #{table} is not found" }
            end
          end
          { result: { delete_tables: [table] } }
        end
      end

      def list_tables
        tables = []

        google_api_client = client.instance_eval { @client }
        response = google_api_client.list_tables(
          config.project_id,
          dataset,
          max_results: max_results
        ).to_h

        while true
          next_page_token = response[:next_page_token]
          tables.concat((response[:tables] || []).map { |t| t[:table_reference][:table_id] })
          # the number of the table is greater than the max_results
          if next_page_token
            response = google_api_client.list_tables(
              config.project_id,
              dataset,
              page_token: next_page_token,
              max_results: max_results
            ).to_h
            # smaller than the max_results
          else
            break
          end
        end

        # sort in descending order (Alphabetically)
        tables.sort.reverse
      end

      def delete_tables(tables)
        purge_before_t = Time.strptime(config.purge_before, config.suffix_format).localtime("+09:00")

        tables.select do |tbl|
          suffix = tbl.gsub(config.table_prefix, '')
          begin
            suffix_t = Time.strptime(suffix, config.suffix_format).localtime("+09:00")
          rescue
            next
          end
          # skip if different from the suffix_format
          next if suffix_t.strftime(config.suffix_format) != suffix
          suffix_t <= purge_before_t
        end
      end
    end

    # action: create_table
    # type: bigquery
    # config:
    #   <<: *bigquery_source
    #   dataset: (required)
    #   table:   (required)
    #   columns: [
    #     { name:, type: } # We DO NOT support NOT NULL, DEFAULT, and so on
    #   ]
    #
    # @return Hash
    # {"responses"=>{
    #   "create_table"=>{
    #     "kind"=>"bigquery#table",
    #      "etag"=>"\"AMpOKMVxm1SnSNRV2IOSXrVvx1s/Lhv0CA4eLg934nJgZqglzdVKBa4\"",
    #      "id"=>"dena-analytics-gcp:medjed_bulk_test.medjeb_bulk_test_20160119",
    #      "selfLink"=>"https://www.googleapis.com/bigquery/v2/projects/dena-analytics-gcp/datasets/medjed_bulk_test/tables/medjeb_bulk_test_20160119",
    #      "tableReference"=>{"projectId"=>"dena-analytics-gcp", "datasetId"=>"medjed_bulk_test", "tableId"=>"medjeb_bulk_test_20160119"},
    #      "schema"=>
    #       {"fields"=>
    #         [{"name"=>"column1", "type"=>"INTEGER"},
    #          {"name"=>"column2", "type"=>"STRING"},
    #          {"name"=>"column3", "type"=>"FLOAT"},
    #          {"name"=>"d", "type"=>"TIMESTAMP"},
    #          {"name"=>"record", "type"=>"RECORD", "fields"=>[{"name"=>"column4", "type"=>"STRING"}, {"name"=>"column5", "type"=>"INTEGER"}]}]},
    #      "numBytes"=>"0",
    #      "numRows"=>"0",
    #      "creationTime"=>"1453210309182",
    #      "lastModifiedTime"=>"1453210309182",
    #      "type"=>"TABLE"}}}
    class CreateTable < ActionBase
      def initialize(*args)
        super
        @schema = Schema.new(config.columns)
      end

      def run
        gem_schema = @schema.gem_schema

        responses = {}
        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) create_table(#{table}, #{gem_schema})" }
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) create_table(#{table}, #{gem_schema})" }
          responses[:create_table] = client.create_table(table, gem_schema)
        end

        { result:  { responses: responses } }
      end
    end

    # action: add_column
    # type: bigquery
    # config:
    #   <<: *bigquery_source
    #   dataset: (required)
    #   table:   (required)
    class AddColumn < ActionBase
      def initialize(*args)
        super
        unless (config.columns or config.add_columns)
          raise Medjed::Bulk::ConfigError, '[BigQuery::AddColumn] `add_columns` or `columns` is required'
        end

        @before_columns = existing_columns
        if config.columns # if already given (by migrate_table)
          @schema = Schema.new(config.columns)
        else
          @schema = Schema.new(config.add_columns)
          @schema.reverse_merge!(@before_columns)
        end
        @schema.validate_permitted_operations!(@before_columns)
      end

      def run
        gem_schema = @schema.gem_schema

        responses = {}
        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) patch_table(#{table}, #{gem_schema})" }
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) patch_table(#{table}, #{gem_schema})" }
          responses[:patch_table] = client.patch_table(table, gem_schema) # raises Google::Apis::ClientError if not exists
        end

        after_columns = existing_columns

        { result:  { responses: responses, before_columns: @before_columns, after_columns: after_columns } }
      end
    end

    # action: drop_column
    # type: bigquery
    # config:
    #   <<: *bigquery_source
    #   dataset: (required)
    #   table:   (required)
    class DropColumn < ActionBase
      def initialize(*args)
        super
        unless (config.columns or config.drop_columns)
          raise Medjed::Bulk::ConfigError, '[BigQuery::DropColumn] `drop_columns` or `columns` is required'
        end

        @before_columns = existing_columns

        if config.columns # if already given (by migrate_table)
          @schema = Schema.new(config.columns)
        else
          @schema = Schema.new(existing_columns)
          @schema.reject_columns!(config.drop_columns)
        end
        if @schema.empty? && !dry_run?
          raise Medjed::Bulk::ConfigError, '[BigQuery::DropColumn] No column is remained'
        end

        @schema.validate_permitted_operations!(@before_columns)

        @result = { responses: {} }
      end

      # create backup dataset if not exist
      # copy table to backup table
      # insert select from table into table (itself)
      def run
        gem_schema = @schema.gem_schema

        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) drop_column(#{table}, #{gem_schema})" }
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) drop_column(#{table}, #{gem_schema})" }
        end

        backup_dataset = "medjed_backup_#{table}" # a backup dataset per table for easy delete
        create_dataset(backup_dataset)

        tsuffix = Time.now.strftime("%Y%m%d%H%M%S%3N")
        backup_table = "#{table}_#{tsuffix}"
        copy(table, backup_table, backup_dataset)

        add_columns_if_necessary

        query_fields = @schema.build_query_fields(@before_columns)
        query = "SELECT #{query_fields.join(',')} FROM [#{dataset}.#{table}]"
        insert_select(query, table)

        after_columns = existing_columns

        { result:  @result.merge!({before_columns: @before_columns, after_columns: after_columns}) }
      end

      def create_dataset(dataset)
        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) create_dataset(#{dataset})" }
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) create_dataset(#{dataset})" }
          unless client.datasets_formatted.include?(dataset) # if not exists
            response = client.create_dataset(dataset)
            @result[:responses][:create_dataset] = response
          end
        end
      end

      def copy(source_table, target_table, target_dataset = nil)
        target_dataset ||= config.dataset
        body_object = {
          'copy' => {
            'sourceTable' => {
              'datasetId' => config.dataset,
              'projectId' => config.project_id,
              'tableId' => source_table,
            },
            'destinationTable' => {
              'datasetId' => target_dataset,
              'projectId' => config.project_id,
              'tableId' => target_table,
            },
          }
        }

        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) copy: insert_job(body_object: #{body_object})" }
          return
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) copy: insert_job(body_object: #{body_object})" }
        end

        response = client.insert_job(body_object)
        @result[:responses][:copy] = response

        get_response = wait_load('copy', response)
        @result[:responses][:copy_job_result] = get_response
      end

      def add_columns_if_necessary
        unless @schema.diff_columns(@before_columns).empty?
          add_column_config = config.dup.tap {|c| c.columns = nil; c.add_columns = @schema.columns }
          result = AddColumn.new(add_column_config).run
          @result[:responses].merge!(result[:responses])
        end
      end

      def insert_select(query, target_table)
        body_object  = {
          'query' => {
            'allowLargeResults' => true,
            'flattenResults' => false,
            'destinationTable' => {
              'datasetId' => config.dataset,
              'projectId' => config.project_id,
              'tableId' => target_table,
            },
            'writeDisposition' => 'WRITE_TRUNCATE',
            'query' => query
          }
        }

        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) query: insert_job(body_object: #{body_object}" }
          return
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) query: insert_job(body_object: #{body_object}" }
        end

        response = client.insert_job(body_object)
        @result[:responses][:query] = response

        get_response = wait_load('query', response)
        @result[:responses][:query_job_result] = get_response
      end

      def wait_load(kind, response)
        started = Time.now

        wait_interval = job_status_polling_interval
        max_polling_time = job_status_max_polling_time
        _response = response

        while true
          job_id = _response['jobReference']['jobId']
          elapsed = Time.now - started
          status = _response['status']['state']
          if status == "DONE"
            Medjed::Bulk.logger.info {
              "(EXECUTE) #{kind} job completed successfully... " \
              "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
            }
            break
          elsif elapsed.to_i > max_polling_time
            message = "(EXECUTE) Checking #{kind} job status... " \
              "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
            Medjed::Bulk.logger.info { message }
            raise JobTimeoutError.new(message)
          else
            Medjed::Bulk.logger.info {
              "(EXECUTE) Checking #{kind} job status... " \
              "job id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
            }
            sleep wait_interval
            _response = client.job(job_id)
          end
        end

        # cf. https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource
        # All errors encountered during the running of the job.
        # Errors here do not necessarily mean that the job has completed or was unsuccessful.
        if _errors = _response['status']['errors']
          Medjed::Bulk.logger.error {
            "(EXECUTE) job(#{job_id}), " \
            "errors:#{_errors.map(&:to_h)}"
          }
          raise Error, "failed during waiting a job, errors:#{_errors.map(&:to_h)}"
        end

        _response
      end
    end

    # Automatically detects add_columns or drop_columns and run
    # bigquery_test: &bigquery_test
    #   client_id: <%= json["client_id"] %>
    #   project_id: <%= json["project_id"] %>
    #   service_email: <%= json["client_email"] %>
    #   key: |-
    #     <%= json["private_key"].split("\n").join("\n    ") %>
    #   dataset: medjed_bulk_test
    #   table: <%= "medjeb_bulk_test_#{Date.today.strftime('%Y%m%d')}" %>
    # 
    # actions:
    #   - action: drop_table
    #     type: bigquery
    #     config:
    #       <<: *bigquery_test
    class MigrateTable < ActionBase
      def initialize(*args)
        super
        if config.schema_file
          config.columns = JSON.parse(File.read(config.schema_file)).map {|e| Hashie::Mash.new(e) }
        end
        @columns = config.columns
        Schema.validate_columns!(@columns)
      end

      def run
        before_columns = existing_columns

        result = {}
        if before_columns.empty?
          result = CreateTable.new(config).run
        else
          target_columns = @columns.dup
          add_columns  = Schema.diff_columns(before_columns, target_columns)
          drop_columns = Schema.diff_columns(target_columns, before_columns)

          if !drop_columns.empty?
            _config = config.dup.tap {|c| c.columns = target_columns }
            result = DropColumn.new(_config).run
          elsif !add_columns.empty?
            _config = config.dup.tap { |c| c.columns = target_columns }
            result = AddColumn.new(_config).run
          end
        end

        after_columns = existing_columns

        if after_columns.empty? and !dry_run?
          raise Medjed::Bulk::Error,
            "BigQuery::MigrateTable: after_columns is empty. " \
            "before_columns: #{before_columns}, after_columns: after_columns, columns: #{@columns}"
        end

        { result:  result.merge!( before_columns: before_columns, after_columns: after_columns ) }
      end
    end

    # action: insert
    # type: bigqeury
    # config:
    #   <<: *bigqeury_source
    #   dataset: (required)
    #   table:  (required)
    # rows:
    #   - id: 1
    #     type: one
    #     record:
    #       child1: 'child1'
    #       child2: 'child2'
    #   - id: 2
    #     type: two
    #     record:
    #       child1: 'child3'
    #       child2: 'child4'
    class Insert < ActionBase
      def initialize(*args)
        super
        @columns = config.columns
        @rows = config.rows
      end

      def run
        unless dry_run?
          client.insert(table, @rows) # no response
        end
        { result:  {} }
      end
    end

    # action: tabledata_list
    # type: bigquery
    # config:
    #   <<: *bigquery_source
    #   dataset: (required)
    #   table:  (required)
    #   max_results: 100 (optional)
    class TabledataList < ActionBase
      def initialize(*args)
        super
        @flattened_existing_columns = Schema.flattened_columns(existing_columns)
      end

      # @return Hash result of tabledata_list
      #
      # Example:
      # {
      #   columns:
      #     [
      #       {
      #         name: id,
      #         type: INTEGER
      #       },
      #       {
      #         name: type,
      #         type: STRING
      #       },
      #       {
      #         name: record.child1,
      #         type: STRING
      #       },
      #       {
      #         name: record.child2,
      #         type: STRING
      #       },
      #   values:
      #     [
      #       "2,two,child3,child4",
      #       "1,one,child1,child2"
      #     ]
      # }
      def run
        if dry_run?
          Medjed::Bulk.logger.info  { "(DRY-RUN) select: tabledata.list(config.table: #{config.table}, client.dataset: #{config.dataset}, maxResults: #{config.max_results})" }
          return
        else
          Medjed::Bulk.logger.info  { "(EXECUTE) select: tabledata.list(config.table: #{config.table}, client.dataset: #{config.dataset}, maxResults: #{config.max_results})" }
          response = client.table_raw_data(config.table, client.dataset, maxResults: config.max_results)
          {
            result:  {
              num_rows: (response['totalRows'] || nil),
              columns: flattened_columns,
              values: flatten_values_to_csv(response['rows'] || [])
            }
          }
        end
      end

      def flattened_columns
        @flattened_existing_columns.map { |k, v| Hashie::Mash.new({name: k}.merge(v)) }
      end

      def flatten_values_to_csv(rows)
        fetch_value_recursively(rows).map { |r| convert_unixtimestamt_to_string(r.flatten).to_csv.chomp! }
      end

      def fetch_value_recursively(rows)
        flattened_rows = rows.map do |r|
          if r.key?('f')
            r['f'].map do |f|
              if f['v'].respond_to?(:key?) && f['v'].key?('f')
                fetch_value_recursively(f['v']['f'])
              else
                f['v']
              end
            end
          else
            r['v']
          end
        end
      end

      def timestamp_indexes
        @timestamp_indexies ||= @flattened_existing_columns.each_with_index.select { |(k, v), i|
          v[:type] == 'TIMESTAMP'
        }.map { |(k, v), i| i }
      end

      def convert_unixtimestamt_to_string(values)
        timestamp_indexes.each do |i|
          values[i] = Time.at(values[i].to_f).utc.strftime('%Y-%m-%d %H:%M:%S.%6N %Z')
        end
        values
      end
    end
  end
end

