# Convert from BigQuery Web console's JavaScript
require_relative 'error'

class BigqueryMigration
  class TableData
    attr_reader :rows
    attr_reader :columns

    def logger
      BigqueryMigration.logger
    end

    def initialize(columns, rows)
      @columns = columns || raise(ConfigError, '`columns` is required.')
      @rows = rows || raise(ConfigError, '`rows` is required.')
    end

    def generate_values
      rows = @rows.map do |row|
        values = []
        max_repeated_count = calculate_repeated_count(columns: @columns, rows: row).max
        max_repeated_count.times do |count|
          values.push(generate_value(columns: @columns, rows: row, count: count))
        end
        values
      end
      # For backword compatibility
      max_row_count = (rows.map(&:length) || []).max
      max_row_count > 1 ? rows : rows.map(&:flatten)
    end

    # This method called recursively.
    # So, rows must be a hash and hash has key f:.
    private def calculate_repeated_count(columns: nil, rows: nil)
      logger.info { "calculate_repeated_count(columns: #{columns}, rows: #{rows})" }
      return [1] if (rows.nil? || rows.empty?)
      validate_rows!(rows)
      rows[:f].zip(columns).map do |row, column|
        if column[:type] == 'RECORD'
          if column[:mode] == 'REPEATED'
            if row[:v].length == 0
              1
            else
              recursive_repeated_counts = row[:v].map do |v|
                _repeated_counts = calculate_repeated_count(columns: column[:fields], rows: v[:v])
                repeated_count = _repeated_counts.inject(0) { |acc, n| [acc, n].max }
                v[:repeated_count] = repeated_count
              end
              recursive_repeated_counts.inject(0) { |acc, n| acc + n }
            end
          else
            _repeated_counts = calculate_repeated_count(columns: column[:fields], rows: row[:v])
            _repeated_counts.inject(0) { |acc, n| [acc, n].max }
          end
        elsif column[:mode] == 'REPEATED'
          [(row[:v] || []).length, 1].max
        else
          1
        end
      end
    end

    # This method called recursively.
    # So, rows must be a hash and hash has key f:.
    private def generate_value(columns: nil, rows: nil, count: nil)
      logger.info { "generate_value(columns: #{columns}, rows: #{rows}, count: #{count})" }
      value = []
      return [nil] if (rows.nil? || rows.empty?)
      validate_rows!(rows)
      rows[:f].zip(columns).each do |row, column|
        if column[:type] == 'RECORD'
          if column[:mode] == 'REPEATED'
            recursive = false
            # Fixme: would like to avoid using the index counter
            current = 0
            row[:v].each do |v|
              repeated_count = v[:repeated_count]
              if current <= count && count < (current + repeated_count)
                generated_values = generate_value(columns: column[:fields], rows: v[:v], count: count - current)
                value.concat(generated_values)
                recursive = true
              end
              current = current + repeated_count
            end
            unless recursive
              nil_count = generate_nil_count(column[:fields])
              value.concat(Array.new(nil_count))
            end
          elsif row[:v].nil?
            nil_count = generate_nil_count(column[:fields])
            value.concat(Array.new(nil_count))
          else
            generated_values = generate_value(columns: column[:fields], rows: row[:v], count: count)
            value.concat(generated_values)
          end
        elsif column[:mode] == 'REPEATED'
          v = row[:v]
          count < v.length ? value.push(normalize_value(v[count][:v])) : value.push(nil)
        elsif count == 0
          value.push((normalize_value(row[:v])))
        else
          value.push(nil)
        end
      end
      value
    end

    # special treatment empty hash.
    # nil is converted into {} by to_h
    private def normalize_value(v)
      v.is_a?(Hash) && v.empty? ? nil : v
    end

    private def generate_nil_count(fields)
      fields.inject(0) do |acc, f|
        f[:type] == 'RECORD' ? acc + generate_nil_count(f[:fields]) : acc + 1
      end
    end

    private def validate_rows!(rows)
      raise ConfigError, '`rows` must be a hash and hash has key `:f`.' if !rows.is_a?(Hash) || !rows.has_key?(:f)
    end
  end
end
