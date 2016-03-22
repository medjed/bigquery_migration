require 'logger'

class BigquerySchemaMigration
  class LogFormatter
    FORMAT = "%s [%s] %s\n"

    def initialize(opts={})
    end

    def call(severity, time, progname, msg)
      FORMAT % [format_datetime(time), severity, format_message(msg)]
    end

    private
    def format_datetime(time)
      time.iso8601
    end

    def format_severity(severity)
      severity
    end

    def format_message(message)
      case message
      when ::Exception
        e = message
        "#{e.class} (#{e.message})\n  #{e.backtrace.join("\n  ")}"
      else
        message.to_s
      end
    end
  end

  class Logger < ::Logger
    def initialize(logdev, shift_age = 0, shift_size = 1048576)
      logdev = STDOUT if logdev == 'STDOUT'
      super(logdev, shift_age, shift_size)
      @formatter = LogFormatter.new
    end

    def write(msg)
      @logdev.write msg
    end
  end
end
