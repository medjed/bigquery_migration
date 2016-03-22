require 'set'
require 'yaml'
require 'erb'
require 'ostruct'

module Medjed
  module Bulk
    class ConfigLoader
      attr_reader :config_path, :namespace

      def initialize(config_path, vars = {})
        @config_path = File.expand_path(config_path)
        @included_files = Set.new
        @namespace = OpenStruct.new(vars)

        # NOTE: In ERB file, you can use variables or methods only defined in `namespace.binding`.
        #       So, the below code adds `include_file` to `namespace.binding`.
        # NOTE: if you share the variables into separated files,
        #       use instance variables like `@hoge`.
        unless @namespace.respond_to?(:include_file)
          itself = self
          @namespace.define_singleton_method(:include_file) do |path|
            # NOTE: pay attention to caller count if you refactor this code.
            # NOTE: the below regex parse the backtrace.
            #       see: http://docs.ruby-lang.org/ja/2.2.0/method/Kernel/m/caller.html
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
          raise IncludeFileInfiniteLoopError, "[Medjed::Bulk::ConfigLoader] `include_file` infinite loop is found"
        end

        raw = File.read(path)
        erb = ERB.new(raw, nil, "-")
        erb.filename = path
        erb.result(namespace.instance_eval { binding })
      end
    end
  end
end
