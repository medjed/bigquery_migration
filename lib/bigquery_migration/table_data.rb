# This codes are translated from BigQuery Web console's JavaScript
require_relative 'error'

class BigqueryMigration
  class TableData
    attr_reader :rows, :columns

    def logger
      BigqueryMigration.logger
    end

    def initialize(columns, rows)
      @columns = columns || raise(Error, '`columns` is required.')
      @rows = rows || raise(Error, '`rows` is required.')
    end

    # format list_table_data response rows which is like
    #
    # [
    #   { f: [
    #     { v: "foo" },
    #     { v: "1" },
    #     { v: [] },
    #     { v: "1.1" },
    #     { v: "true" },
    #     { v: "1.444435200E9" }
    #   ] },
    #   { f: [
    #     { v: "foo" },
    #     { v: "2" },
    #     { v: [
    #       { v: "foo" },
    #       { v: "bar" }
    #     ] },
    #     { v: "2.2" },
    #     { v: "false" },
    #     { v: "1.444435200E9" }
    #   ] }
    # ]
    #
    # into
    #
    # [
    #   # first row
    #   [
    #     [ "foo", "1", nil, "1.1", "true", "1.444435200E9" ]
    #   ],
    #   # second row
    #   [
    #     [ "foo", "2", "foo", "2.2", "false", "1.444435200E9" ],
    #     [ nil, nil, "bar", nil, nil, nil ],
    #   ],
    # ]
    def values
      values = @rows.map do |row|
        repeated_count = repeated_count(columns: @columns, rows: row)
        formatted_row = []
        repeated_count.times do |count|
          formatted_row << format_row(columns: @columns, rows: row, count: count)
        end
        formatted_row
      end
      # flattern if there is no repeated column for backward compatibility
      values.map(&:length).max > 1 ? values : values.flatten(1)
    end

    private

    # Count maximum number of rows on repeated columns
    #
    # This method called recursively, rows must be a hash and hash has key f:
    def repeated_count(columns: nil, rows: nil)
      return 1 if (rows.nil? || rows.empty?)
      validate_rows!(rows)
      rows[:f].zip(columns).map do |row, column|
        if column[:type] == 'RECORD'
          if column[:mode] == 'REPEATED'
            if row[:v].length == 0
              1
            else
              row[:v].map do |v|
                v[:repeated_count] = repeated_count(columns: column[:fields], rows: v[:v])
              end.inject(:+)
            end
          else
            repeated_count(columns: column[:fields], rows: row[:v])
          end
        elsif column[:mode] == 'REPEATED'
          [(row[:v] || []).length, 1].max
        else
          1
        end
      end.max
    end

    # This method called recursively.
    # So, rows must be a hash and hash has key f:.
    def format_row(columns: nil, rows: nil, count: nil)
      formatted_row = []
      return [nil] if (rows.nil? || rows.empty?)
      validate_rows!(rows)
      rows[:f].zip(columns).each do |row, column|
        if column[:type] == 'RECORD'
          if column[:mode] == 'REPEATED'
            recursive = false
            current = 0
            row[:v].each do |v|
              repeated_count = v[:repeated_count]
              if current <= count && count < (current + repeated_count)
                formatted_row.concat format_row(columns: column[:fields], rows: v[:v], count: count - current)
                recursive = true
              end
              current = current + repeated_count
            end
            unless recursive
              nil_count = get_nil_count(column[:fields])
              formatted_row.concat(Array.new(nil_count))
            end
          elsif row[:v].nil?
            nil_count = get_nil_count(column[:fields])
            formatted_row.concat(Array.new(nil_count))
          else
            formatted_row.concat format_row(columns: column[:fields], rows: row[:v], count: count)
          end
        elsif column[:mode] == 'REPEATED'
          v = row[:v]
          count < v.length ? formatted_row.push(normalize_value(v[count][:v])) : formatted_row.push(nil)
        elsif count == 0
          formatted_row.push((normalize_value(row[:v])))
        else
          formatted_row.push(nil)
        end
      end
      formatted_row
    end

    # special treatment empty hash.
    # nil is converted into {} by to_h
    def normalize_value(v)
      v.is_a?(Hash) && v.empty? ? nil : v
    end

    def get_nil_count(fields)
      fields.inject(0) do |acc, f|
        f[:type] == 'RECORD' ? acc + get_nil_count(f[:fields]) : acc + 1
      end
    end

    def validate_rows!(rows)
      raise Error, '`rows` must be a hash and hash has key `:f`.' if !rows.is_a?(Hash) || !rows.has_key?(:f)
    end
  end
end
