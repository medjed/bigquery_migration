require_relative 'config_loader'
require_relative 'error'
require_relative 'action'
require_relative 'hash_util'

class BigquerySchemaMigration
  class ActionRunner
    attr_reader :config, :config_path, :opts

    def initialize(config_path = nil, opts = {})
      @config_path = config_path
      @opts = opts
      config = ConfigLoader.new(@config_path, opts[:vars]).load
      @config = HashUtil.deep_symbolize_keys(config)
      validate_config!
    end

    def run
      success, responses = run_actions
      { success: success, dry_run: @opts[:dry_run], actions: responses }
    end

    def run_actions
      success = true
      responses = []

      @config[:actions].each do |action_config|
        _success, result = Action.new(action_config, @opts).run
        response = action_config.merge({'result' => result})
        responses << response
        unless _success
          success = false
          break
        end
      end

      [success, responses]
    end

    def validate_config!
      unless config.is_a?(Hash)
        raise ConfigError, "config file format has to be YAML Hash"
      end

      unless config[:actions]
        raise ConfigError, "config must have `actions` key"
      end

      unless config[:actions].is_a?(Array)
        raise ConfigError, "config[:actions] must be an Array"
      end

      config[:actions].each do |action_config|
        unless action_config[:action]
          raise ConfigError, "Elements of `config[:actions]` must have `action` key"
        end
      end
    end
  end
end
