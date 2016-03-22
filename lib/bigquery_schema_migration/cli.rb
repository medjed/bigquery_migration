require 'thor'
require 'json'
require_relative '../migrate_table'

module Bigquery
  class MigrateTable
    class CLI < Thor
      # cf. http://qiita.com/KitaitiMakoto/items/c6b9d6311c20a3cc21f9
      def self.exit_on_failure?
        true
      end

      # `run` is reserved by thor, we have to use def _run
      map "run" => "_run"

      # We are not using thor's :default intentionally. See lib/medjed-bulk/option.rb

      option :log_level, :aliases => ["-l"], :type => :string,
        :desc => 'Log level such as fatal, error, warn, info, or debug. (Default: info)'
      option :log, :type => :string,
        :desc => 'Output log to a file (Default: STDOUT)'
      option :exec, :type => :boolean,
        :desc => 'Execute or dry-run (Default: dry-run)'
      option :output, :aliases => ["-o"], :type => :string,
        :desc => 'Output result yaml to a file (Default: STDOUT)'

      # thor's long_desc removes spaces and returns somewhat we don't indend, let me avoid
      # cf. https://github.com/erikhuda/thor/issues/398
      desc 'run <config.yml>', <<-LONGDESC
Run bq_migrate

Example:

  $ cat config.yml.erb
  action: bulkload
  type: file2file
    client_id: 
    project_id: 
    service_email: 
    key: |-
      <%= json["private_key"].split("\n").join("\n    ") %>
    dataset: medjed_bulk_test
    table: <%= "medjeb_bulk_test_#{Date.today.strftime('%Y%m%d')}" %>
  config:
    from: Gemfile
    to: "/tmp/2015-10-21"
  result:
    cmd: cp Gemfile /tmp/test/2015-10-20
    stdout: ''
    stderr: ''
    exit_code: 0
    duration: 0.0033881049894262105
  $ medjed-bulk -c config.yml.erb --vars name:test date:2015-10-20 -o result.yml
LONGDESC
      def _run(config)
        Option.configure(options)
        init_logger
        unless Option[:stdout] == 'STDOUT'
          $stdout.reopen(Option[:stdout])
        end
        unless Option[:stderr] == 'STDERR'
          $stderr.reopen(Option[:stderr])
        end
        $stdout.sync = true
        $stderr.sync = true

        result = Medjed::Bulk::ActionRunner.new(config).run
        open_output do |io|
          io.puts mask_secret(result.deep_stringify_keys.to_yaml)
          Medjed::Bulk.logger.info { "DRY-RUN has finished. Use --exec option to run." } if Option[:dry_run]
        end
        exit(1) unless result[:success]
      end

      private

      def init_logger
        logger = Medjed::Bulk::Logger.new(Option[:log])
        logger.level = Option[:log_level]
        Medjed::Bulk.logger = logger
      end

      def open_output
        output = Option[:output]
        if output == 'STDOUT'
          yield($stdout)
        elsif output == 'STDERR'
          yield($stderr)
        else
          File.open(output, 'w') do |io|
            yield(io)
          end
        end
      end

      def mask_secret(yaml_string)
        %w(password key).each do |secret|
          yaml_string.gsub!(/([^ ]*#{secret}[^ ]*): .*$/, '\1: xxxxx')
        end
        yaml_string.gsub!(/(-----BEGIN\s+PRIVATE\s+KEY-----)[0-9A-Za-z+\/=\s\\]+(-----END\s+PRIVATE\s+KEY-----)/m, '\1 xxxxx \2')
        yaml_string
      end
    end
  end
end
