require 'hashie/mash'
require 'active_support/inflector'
require_relative 'plugin_factory'
require_relative 'config_loader'
require_relative 'error'
require_relative 'option'
require_relative 'meta_plugin/base'

module Medjed
  module Bulk
    # Run actions sequentially. Stop immediately if one of actions fail
    #
    # Return Example: (Actually, ruby Hash)
    #
    # SUCCESS:
    #
    #     ---
    #     success: true
    #     actions:
    #     - action: bulkload
    #       type: file2file
    #       config:
    #         from: Gemfile
    #         to: "/tmp/2015-10-21"
    #       result:
    #         cmd: cp Gemfile /tmp/2015-10-21
    #         stdout: ''
    #         stderr: ''
    #         exit_code: 0
    #         duration: 0.0033881049894262105
    #
    # FAILURE:
    #
    #     ---
    #     success: false
    #     actions:
    #     - action: bulkload
    #       type: file2file
    #       config:
    #         from: Gemfile
    #         to: "/tmp/2015-10-21"
    #       result:
    #         error: 'foo'
    #         error_class: StandardError
    #         error_backtrace:
    #           - ........
    #           - ........
    #           - .......
    class ActionRunner
      attr_reader :config, :config_path, :vars

      def initialize(config_path = nil, vars: nil)
        @config_path = config_path
        @vars ||= vars || Option[:vars]
        config = ConfigLoader.new(@config_path, @vars).load
        config = Hashie::Mash.new(config) if config.is_a?(Hash)
        @config = config
      end

      def run
        validate_config!
        success, responses = run_actions
        { success: success, dry_run: Option[:dry_run], actions: responses }
      end

      # private

      def run_actions
        success = true
        responses = []

        @config.actions.each do |action_config|
          _success, response = run_action(action_config)
          responses << response
          unless _success
            success = false
            break
          end
        end

        [success, responses]
      end

      def run_action(action_config)
        plugin = Medjed::Bulk::PluginFactory.create(action_config, self)
        begin
          success = true
          response = plugin.execute(action_config.action)
        rescue => e
          response = { error: e.message, error_class: e.class.to_s, error_backtrace: e.backtrace }
          success = false
        ensure
          response = action_config.to_hash.merge(response.deep_stringify_keys)
          success = false if response[:success] == false
        end
        [success, response]
      end

      def validate_config!
        unless config.is_a?(Hash)
          raise Medjed::Bulk::ConfigError, 
            "[Medjed::Bulk::ActionRunner] config file format has to be YAML Hash"
        end

        unless config.actions
          raise Medjed::Bulk::ConfigError, 
            "[Medjed::Bulk::ActionRunner] config must have `actions` key"
        end

        unless config.actions.is_a?(Array)
          raise Medjed::Bulk::ConfigError, 
            "[Medjed::Bulk::ActionRunner] the value of `actions` must be an Array"
        end

        config.actions.each do |action_config|
          unless action_config.type
            raise Medjed::Bulk::ConfigError, 
              "[Medjed::Bulk::ActionRunner] elements of `actions` must have `type` key"
          end
          unless action_config.config
            raise Medjed::Bulk::ConfigError, 
              "[Medjed::Bulk::ActionRunner] elements of `actions` must have `config` key"
          end
        end

        # validate whether plugin exists
        config.actions.each do |action_config|
          Medjed::Bulk::PluginFactory.create(action_config, self)
        end

        true
      end
    end
  end
end
