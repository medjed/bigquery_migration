require 'thor'
require 'json'
require 'bigquery_migration'
require_relative 'action_runner'
require_relative 'hash_util'

class BigqueryMigration
  class CLI < Thor
    # cf. http://qiita.com/KitaitiMakoto/items/c6b9d6311c20a3cc21f9
    def self.exit_on_failure?
      true
    end

    # `run` is reserved by thor, we have to use def _run
    map "run" => "_run"

    option :config_path, :aliases => ['-c'], :type => :string,
      :default => 'config.yml'
    option :log_level, :aliases => ["-l"], :type => :string,
      :desc => 'Log level such as fatal, error, warn, info, or debug',
      :default => 'info'
    option :log, :type => :string,
      :desc => 'Output log to a file',
      :default => 'STDOUT'
    option :stdout, :type => :string,
      :desc => 'Redirect STDOUT to a file',
      :default => 'STDOUT'
    option :stderr, :type => :string,
      :desc => 'Redirect STDERR to a file',
      :default => 'STDERR'
    option :exec, :type => :boolean,
      :desc => 'Execute or dry-run (Default: dry-run)',
      :default => false
    option :vars, :type => :hash,
      :desc => 'Variables used in ERB, thor hash format'
    option :output, :aliases => ["-o"], :type => :string,
      :desc => 'Output result yaml to a file',
      :default => 'STDOUT'

    desc 'run <config.yml>', 'run bigquery_migration'
    def _run(config_path)
      opts = options.merge(
        dry_run: !options[:exec]
      )

      init_logger
      reopen_stdout
      reopen_stderr

      result = ActionRunner.new(config_path, opts).run
      open_output do |io|
        io.puts mask_secret(HashUtil.deep_stringify_keys(result).to_yaml)
        logger.info { "DRY-RUN has finished. Use --exec option to run." } if opts[:dry_run]
      end
      exit(1) unless result[:success]
    end

    private

    def logger
      BigqueryMigration.logger
    end

    def init_logger
      logger = BigqueryMigration::Logger.new(options[:log])
      logger.level = options[:log_level]
      BigqueryMigration.logger = logger
    end

    def reopen_stdout
      unless options[:stdout] == 'STDOUT'
        $stdout.reopen(options[:stdout])
      end
      $stdout.sync = true
    end

    def reopen_stderr
      unless options[:stderr] == 'STDERR'
        $stderr.reopen(options[:stderr])
      end
      $stderr.sync = true
    end

    def open_output
      output = options[:output]
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
        yaml_string.gsub!(/([^ ]*#{secret}): .*$/, '\1: xxxxx')
      end
      yaml_string.gsub!(/(-----BEGIN\s+PRIVATE\s+KEY-----)[0-9A-Za-z+\/=\s\\]+(-----END\s+PRIVATE\s+KEY-----)/m, '\1 xxxxx \2')
      yaml_string
    end
  end
end
