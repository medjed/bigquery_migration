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

    def generate_table_rows
      rows = @rows.map do |row|
        table_rows = []
        max_repeated_count = calculate_repeated_count(columns: @columns, rows: row).max
        max_repeated_count.times do |count|
          table_rows.push(generate_table_row(columns: @columns, rows: row, count: count))
        end
        table_rows
      end
      # For backword compatibility
      max_row_count = (rows.map(&:length) || []).max
      max_row_count > 1 ? rows : rows.map(&:flatten)
    end

    # This method called recursively.
    # So, rows must be a hash and hash has key f:.
    private def calculate_repeated_count(columns: nil, rows: nil)
      logger.info { "calculate_repeated_count(columns: #{columns}, rows: #{rows})" }
      return [1] if rows.nil?
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
    private def generate_table_row(columns: nil, rows: nil, count: nil)
      logger.info { "generate_table_row(columns: #{columns}, rows: #{rows}, count: #{count})" }
      table_row = []
      return table_row if rows.nil?
      rows[:f].zip(columns).each do |row, column|
        if column[:type] == 'RECORD'
          if column[:mode] == 'REPEATED'
            recursive = false
            # Fixme: would like to avoid using the index counter
            current = 0
            row[:v].each do |v|
              repeated_count = v[:repeated_count]
              if current <= count && count < (current + repeated_count)
                generated_table_rows = generate_table_row(columns: column[:fields], rows: v[:v], count: count - current)
                table_row.concat(generated_table_rows)
                recursive = true
              end
              current = current + repeated_count
            end
            unless recursive
              nil_count = generate_nil_count(column[:fields])
              table_row.concat(Array.new(nil_count))
            end
          elsif row[:v].nil?
            nil_count = generate_nil_count(column[:fields])
            table_row.concat(Array.new(nil_count))
          else
            generated_table_rows = generate_table_row(columns: column[:fields], rows: row[:v], count: count)
            table_row.concat(generated_table_rows)
          end
        elsif column[:mode] == 'REPEATED'
          v = row[:v]
          count < v.length ? table_row.push(v[count][:v]) : table_row.push(nil)
        elsif count == 0
          table_row.push(row[:v])
        else
          table_row.push(nil)
        end
      end
      table_row
    end

    private def generate_nil_count(fields)
      fields.inject(0) do |acc, f|
        f[:type] == 'RECORD' ? acc + generate_nil_count(f[:fields]) : acc + 1
      end
    end
  end
end
