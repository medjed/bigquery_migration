require 'set'
require 'yaml'
require 'erb'
require 'ostruct'

class BigquerySchemaMigration
  class ConfigLoader
    attr_reader :config_path, :namespace

    class AlreayIncluded < ::StandardError; end

    def initialize(config_path, vars = {})
      @config_path = File.expand_path(config_path)
      @included_files = Set.new
      @namespace = OpenStruct.new(vars)

      unless @namespace.respond_to?(:include_file)
        itself = self
        # ToDo: better way?
        @namespace.define_singleton_method(:include_file) do |path|
          caller_path = caller[0][/^([^:]+):\d+:in `[^']*'$/, 1]
          abs_path = File.expand_path(path, File.dirname(caller_path))
          if File.extname(path) == '.erb'
            itself.load_erb(abs_path)
          else
            File.read(abs_path)
          end
        end
      end
    end

    def load
      if File.extname(config_path) == '.erb'
        YAML.load(load_erb(config_path))
      else
        YAML.load(File.read(config_path))
      end
    end

    def load_erb(path = config_path)
      unless @included_files.add?(path)
        raise AlreayIncluded, "#{path} was included twice"
      end

      raw = File.read(path)
      erb = ERB.new(raw, nil, "-")
      erb.filename = path
      erb.result(namespace.instance_eval { binding })
    end
  end
end
