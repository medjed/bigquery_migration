require 'csv'
require 'json'
require_relative 'schema'
require_relative 'table_data'
require_relative 'error'
require_relative 'time_with_zone'
require_relative 'hash_util'
require 'google/apis/bigquery_v2'
require 'google/api_client/auth/key_utils'
require 'securerandom'
require 'inifile'

class BigqueryMigration
  class BigqueryWrapper
    attr_reader :config

    def logger
      BigqueryMigration.logger
    end

    def initialize(config, opts = {})
      @config = HashUtil.deep_symbolize_keys(config)
      @opts = HashUtil.deep_symbolize_keys(opts)
    end

    def client
      return @cached_client if @cached_client && @cached_client_expiration > Time.now

      client = Google::Apis::BigqueryV2::BigqueryService.new
      client.request_options.retries = retries
      client.client_options.open_timeout_sec = open_timeout_sec
      if client.request_options.respond_to?(:timeout_sec)
        client.request_options.timeout_sec = timeout_sec
      else # google-api-ruby-client >= v0.11.0
        if timeout_sec
          logger.warn { "timeout_sec is deprecated in google-api-ruby-client >= v0.11.0. Use read_timeout_sec instead" }
        end
        client.client_options.send_timeout_sec = send_timeout_sec
        client.client_options.read_timeout_sec = read_timeout_sec
      end
      logger.debug { "client_options: #{client.client_options.to_h}" }
      logger.debug { "request_options: #{client.request_options.to_h}" }

      scope = "https://www.googleapis.com/auth/bigquery"

      case auth_method
      when 'authorized_user'
        auth = Signet::OAuth2::Client.new(
          token_credential_uri: "https://accounts.google.com/o/oauth2/token",
          audience: "https://accounts.google.com/o/oauth2/token",
          scope: scope,
          client_id:     credentials[:client_id],
          client_secret: credentials[:client_secret],
          refresh_token: credentials[:refresh_token]
        )
        auth.refresh!
      when 'compute_engine'
        auth = Google::Auth::GCECredentials.new
      when 'service_account'
        key = StringIO.new(credentials.to_json)
        auth = Google::Auth::ServiceAccountCredentials.make_creds(json_key_io: key, scope: scope)
      when 'application_default'
        auth = Google::Auth.get_application_default([scope])
      else
        raise ConfigError, "Unknown auth method: #{auth_method}"
      end

      client.authorization = auth

      @cached_client_expiration = Time.now + 1800
      @cached_client = client
    end

    def existing_columns
      begin
        result = get_table
        response = result[:responses][:get_table]
        return [] unless response
        return [] unless response.schema
        return [] unless response.schema.fields
        response.schema.fields.map {|column| column.to_h }
      rescue NotFoundError
        return []
      end
    end

    def get_dataset(dataset: nil)
      dataset ||= self.dataset
      begin
        logger.info { "Get dataset... #{project}:#{dataset}" }
        response = client.get_dataset(project, dataset)
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404
          raise NotFoundError, "Dataset #{project}:#{dataset} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        raise Error, "Failed to get_dataset(#{project}, #{dataset}), response:#{response}"
      end

      { responses: { get_dataset: response } }
    end

    def insert_dataset(dataset: nil, reference: nil)
      dataset ||= self.dataset
      begin
        logger.info { "#{head}Insert (create) dataset... #{project}:#{dataset}" }
        hint = {}
        if reference
          response = get_dataset(reference)
          hint = { access: response.access }
        end
        body = {
          dataset_reference: {
            project_id: project,
            dataset_id: dataset,
          },
        }.merge(hint)
        body[:location] = location if location
        opts = {}

        logger.debug { "#{head}insert_dataset(#{project}, #{body}, #{opts})" }
        unless dry_run?
          response = client.insert_dataset(project, body, opts)
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 409 && /Already Exists:/ =~ e.message
          # ignore 'Already Exists' error
          return {}
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        raise Error, "Failed to insert_dataset(#{project}, #{body}, #{opts}), response:#{response}"
      end

      { responses: { insert_dataset: response } }
    end
    alias :create_dataset :insert_dataset

    def get_table(dataset: nil, table: nil)
      dataset ||= self.dataset
      table ||= self.table
      begin
        logger.debug { "Get table... #{project}:#{dataset}.#{table}" }
        response = client.get_table(project, dataset, table)
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 # not found
          raise NotFoundError, "Table #{project}:#{dataset}.#{table} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        raise Error, "Failed to get_table(#{project}, #{dataset}, #{table}), response:#{response}"
      end

      result = {}
      if response
        result = {
          table_id: response.id,
          creation_time: response.creation_time.to_i, # millisec
          last_modified_time: response.last_modified_time.to_i, # millisec
          location: response.location,
          num_bytes: response.num_bytes.to_i,
          num_rows: response.num_rows.to_i,
        }
      end

      result.merge!({ responses: { get_table: response } })
    end

    def insert_table(dataset: nil, table: nil, columns:, options: {})
      dataset ||= self.dataset
      table ||= self.table
      raise Error, "columns is empty" if columns.empty?
      schema = Schema.new(columns)

      begin
        logger.info { "#{head}Insert (create) table... #{project}:#{dataset}.#{table}" }
        body = {
          table_reference: {
            table_id: table,
          },
          schema: {
            fields: schema,
          }
        }

        if options['time_partitioning']
          body[:time_partitioning] = {
            type: options['time_partitioning']['type'],
            expiration_ms: options['time_partitioning']['expiration_ms'],
          }
        end

        if clustering && clustering[:fields]
          body[:clustering] = {
            fields: clustering[:fields]
          }
        end

        opts = {}
        logger.debug { "#{head}insert_table(#{project}, #{dataset}, #{body}, #{opts})" }
        unless dry_run?
          response = client.insert_table(project, dataset, body, opts)
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 409 && /Already Exists:/ =~ e.message
          # ignore 'Already Exists' error
          return {}
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        raise Error, "Failed to insert_table(#{project}, #{dataset}, #{body}, #{opts}), response:#{response}"
      end

      { responses: { insert_table: response } }
    end
    alias :create_table :insert_table

    def insert_partitioned_table(dataset: nil, table: nil, columns:, options: {})
      options['time_partitioning'] = {'type'=>'DAY'}
      insert_table(dataset: dataset, table: table, columns: columns, options: options)
    end
    alias :create_partitioned_table :insert_partitioned_table

    def delete_table(dataset: nil, table: nil)
      dataset ||= self.dataset
      table ||= self.table

      begin
        logger.info { "#{head}Delete (drop) table... #{project}:#{dataset}.#{table}" }
        unless dry_run?
          client.delete_table(project, dataset, table) # no response
          success = true
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 && /Not found:/ =~ e.message
          # ignore 'Not Found' error
          return {}
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        raise Error, "Failed to delete_table(#{project}, #{dataset}, #{table}), response:#{response}"
      end

      { success: success }
    end
    alias :drop_table :delete_table

    def list_tables(dataset: nil, max_results: 999999)
      dataset ||= self.dataset

      tables = []
      begin
        logger.info { "List tables... #{project}:#{dataset}" }
        response = client.list_tables(project, dataset, max_results: max_results)
        while true
          _tables = (response.tables || []).map { |t| t.table_reference.table_id.to_s }
          tables.concat(_tables)
          if next_page_token = response.next_page_token
            response = client.list_tables(project, dataset, page_token: next_page_token, max_results: max_results)
          else
            break
          end
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 && /Not found:/ =~ e.message
          raise NotFoundError, "Dataset #{project}:#{dataset} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        logger.error { "list_tables(#{project}, #{dataset}), response:#{response}" }
        raise Error, "failed to list tables #{project}:#{dataset}, response:#{response}"
      end

      { tables: tables }
    end

    def purge_tables(dataset: nil, table_prefix: , suffix_format: , purge_before: , timezone: nil)
      dataset ||= self.dataset
      timezone ||= Time.now.strftime('%z')

      before_tables = list_tables[:tables]

      purge_before_t = TimeWithZone.strptime_with_zone(purge_before, suffix_format, timezone)
      tables = before_tables.select do |tbl|
        suffix = tbl.gsub(table_prefix, '')
        begin
          suffix_t = TimeWithZone.strptime_with_zone(suffix, suffix_format, timezone)
        rescue
          next
        end
        # skip if different from the suffix_format
        next if suffix_t.strftime(suffix_format) != suffix
        suffix_t <= purge_before_t
      end

      tables.each do |_table|
        delete_table(table: _table)
        # If you make more than 100 requests per second, throttling might occur.
        # See https://cloud.google.com/bigquery/quota-policy#apirequests
        sleep 1
      end

      { delete_tables: tables }
    end

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
    def insert_all_table_data(dataset: nil, table: nil, rows: )
      dataset ||= self.dataset
      table ||= self.table

      begin
        logger.info { "#{head}insertAll tableData... #{project}:#{dataset}.#{table}" }
        body = {
          rows: rows.map {|row| { json: row } },
        }
        opts = {}
        unless dry_run?
          response = client.insert_all_table_data(project, dataset, table, body, opts)
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 # not found
          raise NotFoundError, "Table #{project}:#{dataset}.#{table} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        Medjed::Bulk.logger.error {
          "insert_all_table_data(#{project}, #{dataset}, #{table}, #{opts}), response:#{response}"
        }
        raise Error, "failed to insert_all table_data #{project}:#{dataset}.#{table}, response:#{response}"
      end

      { responses: { insert_all_table_data: response } }
    end

    # @return Hash result of list table_data
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
    #       [2,"two","child3","child4"],
    #       [1,"one","child1","child2"]
    #     ],
    #   total_rows: 2
    # }
    def list_table_data(dataset: nil, table: nil, max_results: 100)
      dataset ||= self.dataset
      table ||= self.table

      begin
        logger.info  { "list_table_data(#{project}, #{dataset}, #{table}, max_results: #{max_results})" }
        response = client.list_table_data(project, dataset, table, max_results: max_results)
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 # not found
          raise NotFoundError, "Table #{project}:#{dataset}.#{table} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        logger.error  { "list_table_data(#{project}, #{dataset}, #{table}, max_results: #{max_results})" }
        raise Error, "Failed to list table_data #{project}:#{dataset}.#{table}, response:#{response}"
      end

      columns = existing_columns
      flattened_columns = Schema.new(columns).flattened_columns.map do |name, column|
        {name: name}.merge!(column)
      end
      if rows = response.to_h[:rows]
        values = TableData.new(columns, rows).values
      end

      {
        total_rows: response.total_rows,
        columns: flattened_columns,
        values: values,
        responses: {
          list_table_data: response,
        }
      }
    end

    def patch_table(dataset: nil, table: nil, columns: nil, add_columns: nil)
      dataset ||= self.dataset
      table ||= self.table

      if columns.nil? and add_columns.nil?
        raise ArgumentError, 'patch_table: `columns` or `add_columns` is required'
      end

      before_columns = existing_columns
      if columns # if already given
        schema = Schema.new(columns)
      else
        schema = Schema.new(add_columns)
        schema.reverse_merge!(before_columns)
      end
      schema.validate_permitted_operations!(before_columns)

      begin
        logger.info { "#{head}Patch table... #{project}:#{dataset}.#{table}" }
        fields = schema.map {|column| HashUtil.deep_symbolize_keys(column) }
        body = {
          schema: {
            fields: fields,
          }
        }
        opts = {}
        logger.debug { "#{head}patch_table(#{project}, #{dataset}, #{table}, #{body}, options: #{opts})" }
        unless dry_run?
          response = client.patch_table(project, dataset, table, body, options: opts)
        end
      rescue Google::Apis::ServerError, Google::Apis::ClientError, Google::Apis::AuthorizationError => e
        if e.status_code == 404 # not found
          raise NotFoundError, "Table #{project}:#{dataset}.#{table} is not found"
        end

        response = {status_code: e.status_code, message: e.message, error_class: e.class}
        logger.error {
          "patch_table(#{project}, #{dataset}, #{table}, #{body}, options: #{opts}), response:#{response}"
        }
        raise Error, "Failed to patch table #{project}:#{dataset}.#{table}, response:#{response}"
      end

      after_columns = existing_columns

      {
        before_columns: before_columns,
        after_columns:  after_columns,
        responses: { patch_table: response },
      }
    end
    alias :add_column :patch_table

    def copy_table(destination_table:, destination_dataset: nil, source_table: nil, source_dataset: nil, write_disposition: nil)
      source_table ||= self.table
      source_dataset ||= self.dataset
      destination_dataset ||= source_dataset
      write_disposition ||= 'WRITE_TRUNCATE'

      body = {
        job_reference: {
          project_id: self.project,
          job_id: "job_#{SecureRandom.uuid}",
        },
        configuration: {
          copy: {
            create_deposition: 'CREATE_IF_NEEDED',
            write_disposition: write_disposition,
            source_table: {
              project_id: project,
              dataset_id: source_dataset,
              table_id: source_table,
            },
            destination_table: {
              project_id: project,
              dataset_id: destination_dataset,
              table_id: destination_table,
            },
          }
        }
      }
      body[:job_reference][:location] = location if location
      opts = {}

      logger.info  { "#{head}insert_job(#{project}, #{body}, #{opts})" }
      unless dry_run?
        response = client.insert_job(project, body, opts)
        get_response = wait_load('copy', response)
      end

      {
        responses: {
          insert_job: response,
          last_get_job: get_response,
        }
      }
    end

    def insert_select(query:, destination_table: nil, destination_dataset: nil, write_disposition: nil)
      destination_table   ||= self.table
      destination_dataset ||= self.dataset
      write_disposition ||= 'WRITE_TRUNCATE'

      body  = {
        job_reference: {
          project_id: self.project,
          job_id: "job_#{SecureRandom.uuid}",
        },
        configuration: {
          query: {
            allow_large_results: true,
            flatten_results: false,
            write_disposition: write_disposition,
            query: query,
            destination_table: {
              project_id: self.project,
              dataset_id: destination_dataset,
              table_id: destination_table,
            },
          }
        }
      }
      body[:job_reference][:location] = location if location
      opts = {}

      logger.info { "#{head}insert_job(#{project}, #{body}, #{opts})" }
      unless dry_run?
        response = client.insert_job(project, body, opts)
        get_response = wait_load('query', response)
      end

      {
        responses: {
          insert_job: response,
          last_get_job: get_response,
        }
      }
    end

    def wait_load(kind, response)
      started = Time.now

      wait_interval = self.job_status_polling_interval
      max_polling_time = self.job_status_max_polling_time
      _response = response

      while true
        job_id = _response.job_reference.job_id
        elapsed = Time.now - started
        status = _response.status.state
        if status == "DONE"
          logger.info {
            "#{kind} job completed... " \
            "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
          }
          break
        elsif elapsed.to_i > max_polling_time
          message = "#{kind} job checking... " \
            "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[TIMEOUT]"
          logger.info { message }
          raise JobTimeoutError.new(message)
        else
          logger.info {
            "#{kind} job checking... " \
            "job_id:[#{job_id}] elapsed_time:#{elapsed.to_f}sec status:[#{status}]"
          }
          sleep wait_interval
          if support_location_keyword?
            _response = client.get_job(project, job_id, location: location)
          else
            _response = client.get_job(project, job_id)
          end
        end
      end

      # cf. http://www.rubydoc.info/github/google/google-api-ruby-client/Google/Apis/BigqueryV2/JobStatus#errors-instance_method
      # `errors` returns Array<Google::Apis::BigqueryV2::ErrorProto> if any error exists.
      # Otherwise, this returns nil.
      if _errors = _response.status.errors
        raise Error, "Failed during waiting a job, get_job(#{project}, #{job_id}), errors:#{_errors.map(&:to_h)}"
      end

      _response
    end

    def drop_column(table: nil, columns: nil, drop_columns: nil, backup_dataset: nil, backup_table: nil)
      table ||= self.table
      backup_dataset ||= self.dataset
      if columns.nil? and drop_columns.nil?
        raise ArgumentError, '`drop_columns` or `columns` is required'
      end

      result = { responses: {} }

      before_columns = existing_columns

      if columns # if already given
        schema = Schema.new(columns)
      else
        schema = Schema.new(existing_columns)
        schema.reject_columns!(drop_columns)
      end
      if schema.empty? && !dry_run?
        raise Error, 'No column is remained'
      end

      schema.validate_permitted_operations!(before_columns)

      unless backup_dataset == self.dataset
        create_dataset(dataset: backup_dataset)
      end

      if backup_table
        _result = copy_table(source_table: table, destination_table: backup_table, destination_dataset: backup_dataset)
        result[:responses].merge!(_result[:responses])
      end

      unless (add_columns = schema.diff_columns_by_name(before_columns)).empty?
        _result = patch_table(add_columns: add_columns)
        result[:responses].merge!(_result[:responses])
      end

      query_fields = schema.build_query_fields(before_columns)
      query = "SELECT #{query_fields.join(',')} FROM [#{dataset}.#{table}]"
      _result = insert_select(query: query, destination_table: table)
      result[:responses].merge!(_result[:responses])

      after_columns = existing_columns

      result.merge!({before_columns: before_columns, after_columns: after_columns})
    end

    def migrate_table(table: nil, schema_file: nil, columns: nil, backup_dataset: nil, backup_table: nil)
      table ||= self.table
      backup_dataset ||= self.dataset

      if schema_file.nil? and columns.nil?
        raise ArgumentError, '`schema_file` or `columns` is required'
      end
      if schema_file
        columns = HashUtil.deep_symbolize_keys(JSON.parse(File.read(schema_file)))
      end
      Schema.validate_columns!(columns)

      result = {}
      begin
        get_table
      rescue NotFoundError
        before_columns = []
        result = create_table(table: table, columns: columns)
      else
        before_columns = existing_columns
        add_columns  = Schema.diff_columns(before_columns, columns)
        drop_columns = Schema.diff_columns(columns, before_columns)

        if !drop_columns.empty?
          drop_column(table: table, columns: columns,
                      backup_dataset: backup_dataset, backup_table: backup_table)
        elsif !add_columns.empty?
          add_column(table: table, columns: columns)
        end
      end

      after_columns = existing_columns

      if after_columns.empty? and !dry_run?
        raise Error, "after_columns is empty. " \
          "before_columns: #{before_columns}, after_columns: #{after_columns}, columns: #{columns}"
      end

      result.merge!( before_columns: before_columns, after_columns: after_columns )
    end

    # creates a table with time_partitioning option
    # this version only uses patch table API (no query job) because querying partitioned table should cost lots
    def migrate_partitioned_table(table: nil, schema_file: nil, columns: nil, options: {})
      table ||= self.table

      if schema_file.nil? and columns.nil?
        raise ArgumentError, '`schema_file` or `columns` is required'
      end
      if schema_file
        columns = HashUtil.deep_symbolize_keys(JSON.parse(File.read(schema_file)))
      end
      Schema.validate_columns!(columns)

      result = {}
      begin
        get_table
      rescue NotFoundError
        before_columns = []
        result = create_partitioned_table(table: table, columns: columns, options: options)
      else
        before_columns = existing_columns
        add_columns  = Schema.diff_columns(before_columns, columns)
        drop_columns = Schema.diff_columns(columns, before_columns)

        if !drop_columns.empty? || !add_columns.empty?
          Schema.make_nullable!(drop_columns) # drop columns will be NULLABLE columns
          Schema.reverse_merge!(columns, patch_columns = drop_columns)
          Schema.reverse_merge!(patch_columns, patch_columns = add_columns)
          patch_table(table: table, columns: patch_columns)
        end
      end

      after_columns = existing_columns

      if after_columns.empty? and !dry_run?
        raise Error, "after_columns is empty. " \
          "before_columns: #{before_columns}, after_columns: #{after_columns}, columns: #{columns}"
      end

      result.merge!( before_columns: before_columns, after_columns: after_columns )
    end

    # the location keyword arguments are available in google-api-client v0.19.6 or later
    def support_location_keyword?
      @support_location_keyword ||= client.method(:get_job).parameters.include?([:key, :location])
    end

    # For old version compatibility
    # Use credentials_file or credentials instead
    def json_key
      if json_keyfile = config[:json_keyfile]
        begin
          case json_keyfile
          when String
            return HashUtil.deep_symbolize_keys(JSON.parse(File.read(json_keyfile)))
          when Hash
            case json_keyfile[:content]
            when String
              return HashUtil.deep_symbolize_keys(JSON.parse(json_keyfile[:content]))
            when Hash
              return json_keyfile[:content]
            else
              raise ConfigError.new "Unsupported json_keyfile type"
            end
          else
            raise ConfigError.new "Unsupported json_keyfile type"
          end
        rescue => e
          raise ConfigError.new "json_keyfile is not a JSON file"
        end
      end
      nil
    end

    # compute_engine, authorized_user, service_account
    def auth_method
      @auth_method ||= ENV['AUTH_METHOD'] || config.fetch(:auth_method, nil) || credentials[:type] || 'compute_engine'
    end

    def credentials
      json_key || HashUtil.deep_symbolize_keys(JSON.parse(config.fetch(:credentials, nil) || File.read(credentials_file)))
    end

    def credentials_file
      @credentials_file ||= File.expand_path(
        # ref. https://developers.google.com/identity/protocols/application-default-credentials
        ENV['GOOGLE_APPLICATION_CREDENTIALS'] ||
        config.fetch(:credentials_file, nil) ||
        (File.exist?(global_application_default_credentials_file) ? global_application_default_credentials_file : application_default_credentials_file)
      )
    end

    def application_default_credentials_file
      @application_default_credentials_file ||= File.expand_path("~/.config/gcloud/application_default_credentials.json")
    end

    def global_application_default_credentials_file
      @global_application_default_credentials_file ||= '/etc/google/auth/application_default_credentials.json'
    end

    def config_default_file
      File.expand_path('~/.config/gcloud/configurations/config_default')
    end

    def config_default
      # {core:{account:'xxx',project:'xxx'},compute:{zone:'xxx}}
      @config_default ||= File.readable?(config_default_file) ? HashUtil.deep_symbolize_keys(IniFile.load(config_default_file).to_h) : {}
    end

    def service_account_default
      (config_default[:core] || {})[:account]
    end

    def project_default
      (config_default[:core] || {})[:project]
    end

    def zone_default
      (config_default[:compute] || {})[:zone]
    end

    def service_account
      @service_account ||= ENV['GOOGLE_SERVICE_ACCOUNT'] || config.fetch(:service_account, nil) || credentials[:client_email] || service_account_default
    end

    def retries
      @retries ||= ENV['RETRIES'] || config.fetch(:retries, nil) || 5
    end

    # For google-api-client < 0.11.0. Deprecated
    def timeout_sec
      @timeout_sec ||= ENV['TIMEOUT_SEC'] || config.fetch(:timeout_sec, nil)
    end

    def send_timeout_sec
      @send_timeout_sec ||= ENV['SEND_TIMEOUT_SEC'] || config.fetch(:send_timeout_sec, nil) || 60
    end

    def read_timeout_sec
      @read_timeout_sec ||= ENV['READ_TIMEOUT_SEC'] || config.fetch(:read_timeout_sec, nil) || timeout_sec || 300
    end

    def open_timeout_sec
      @open_timeout_sec ||= ENV['OPEN_TIMEOUT_SEC'] || config.fetch(:open_timeout_sec, nil) || 300
    end

    def project
      @project ||= ENV['GOOGLE_PROJECT'] || config.fetch(:project, nil) || credentials[:project_id]
      @project ||= credentials[:client_email].chomp('.iam.gserviceaccount.com').split('@').last if credentials[:client_email]
      @project ||= project_default || raise(ConfigError, '`project` is required.')
    end

    def dataset
      @dataset ||= config[:dataset] || raise(ConfigError, '`dataset` is required.')
    end

    def table
      @table  ||= config[:table]   || raise(ConfigError, '`table` is required.')
    end

    def location
      config[:location]
    end

    def clustering
      config[:clustering]
    end

    def job_status_polling_interval
      @job_status_polling_interval ||= config[:job_status_polling_interval] || 5
    end

    def job_status_max_polling_time
      @job_status_max_polling_time ||= config[:job_status_polling_time] || 3600
    end

    def dry_run?
      @opts[:dry_run]
    end

    def head
      dry_run? ? '(DRY-RUN) ' : '(EXECUTE) '
    end
  end
end
