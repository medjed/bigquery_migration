require 'csv'
require 'json'
require_relative 'error'

class BigqueryMigration
  class Schema < ::Array
    ALLOWED_FIELD_TYPES = Set.new(['STRING', 'INTEGER', 'FLOAT', 'BOOLEAN', 'RECORD', 'TIMESTAMP', 'BYTES', 'DATE', 'TIME', 'DATETIME'])
    ALLOWED_FIELD_MODES = Set.new(['NULLABLE', 'REQUIRED', 'REPEATED'])

    def initialize(columns = [])
      normalized = self.class.normalize_columns(columns)
      super(normalized)
      validate_columns!
    end

    def find_column_by_name(name)
      self.class.find_column_by_name(self, name)
    end

    def validate_columns!
      self.class.validate_columns!(self)
    end

    def validate_permitted_operations!(source_columns)
      target_columns = self
      self.class.validate_permitted_operations!(source_columns, target_columns)
    end

    def normalize_columns
      self.class.normalize_columns(self)
    end

    def shallow_normalize_columns
      self.class.shallow_normalize_columns(self)
    end
    def shallow_normalize_columns!
      self.class.shallow_normalize_column!(self)
    end

    def flattened_columns
      self.class.flattened_columns(self)
    end

    def equals?(source_columns)
      self.class.equals?(source_columns, self)
    end

    # self - source_columns
    def diff_columns(source_columns)
      self.class.diff_columns(source_columns, self)
    end

    # diff with only column names
    # self - source_columns
    def diff_columns_by_name(source_columns)
      self.class.diff_columns_by_name(source_columns, self)
    end

    # A.merge!(B) => B overwrites A
    # A.reverse_merge!(B) => A overwrites B, but A is modified
    def reverse_merge!(source_columns)
      self.class.reverse_merge!(source_columns, self)
    end

    def reject_columns!(drop_columns)
      self.class.reject_columns!(drop_columns, self)
    end

    def build_query_fields(source_columns)
      self.class.build_query_fields(source_columns, self)
    end

    class << self
      # The name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
      # and must start with a letter or underscore. The maximum length is 128 characters.
      def validate_name!(name)
        unless name =~ /\A[a-zA-Z_]+\w*\Z/
          raise ConfigError, "Column name `#{name}` is invalid format"
        end
        unless name.length < 128
          raise ConfigError, "Column name `#{name}` must be less than 128"
        end
      end

      def validate_type!(type)
        unless ALLOWED_FIELD_TYPES.include?(type.upcase)
          raise ConfigError, "Column type `#{type}` is not allowed type"
        end
      end

      def validate_mode!(mode)
        unless ALLOWED_FIELD_MODES.include?(mode.upcase)
          raise ConfigError, "Column mode `#{mode}` is not allowed mode"
        end
      end

      def validate_columns!(columns)
        columns.each do |column|
          validate_name!(column[:name])
          validate_type!(column[:type])
          validate_mode!(column[:mode]) if column[:mode]

          if column[:type] == 'RECORD'
            validate_columns!(column[:fields])
          end
        end
      end

      def find_column_by_name(columns, name)
        (columns || []).find { |c| c[:name] == name }
      end

      # validates permitted changes from old schema to new schema
      def validate_permitted_operations!(source_columns, target_columns)
        flattened_source_columns = flattened_columns(normalize_columns(source_columns))
        flattened_target_columns = flattened_columns(normalize_columns(target_columns))

        flattened_target_columns.keys.each do |flattened_name|
          next unless flattened_source_columns.key?(flattened_name)
          validate_permitted_operations_for_type!(
            flattened_source_columns[flattened_name],
            flattened_target_columns[flattened_name]
          )
          validate_permitted_operations_for_mode!(
            flattened_source_columns[flattened_name],
            flattened_target_columns[flattened_name]
          )
        end
      end

      # @param [Hash] source_column
      # @param [Hash] target_column
      #
      # Disallowed conversion rule is as follows:
      #
      #   type: RECORD => type: others
      #   mode: REPEATED => change type
      #
      def validate_permitted_operations_for_type!(source_column, target_column)
        source_column = shallow_normalize_column(source_column)
        target_column = shallow_normalize_column(target_column)

        msg = "(#{source_column.to_h} => #{target_column.to_h})"
        if source_column[:type] == 'RECORD'
          if target_column[:type] != 'RECORD'
            raise ConfigError, "`RECORD` can not be changed #{msg}"
          end
        end
        if source_column[:mode] and source_column[:mode] == 'REPEATED'
          if source_column[:type] != target_column[:type]
            raise ConfigError, "`REPEATED` mode column's type can not be changed #{msg}"
          end
        end
      end

      # @param [Hash] source_column
      # @param [Hash] target_column
      #
      # Allowed conversion rule is as follows:
      #
      #     (new)    => NULLABLE, REPEATED
      #     NULLABLE => NULLABLE
      #     REQUIRED => REQUIRED, NULLABLE
      #     REPEATED => REPEATED
      def validate_permitted_operations_for_mode!(source_column, target_column)
        source_column = shallow_normalize_column(source_column)
        target_column = shallow_normalize_column(target_column)
        source_mode   = source_column[:mode]
        target_mode   = target_column[:mode]

        return if source_mode == target_mode
        msg = "(#{source_column.to_h} => #{target_column.to_h})"

        case source_mode
        when nil
          if target_mode == 'REQUIRED'
            raise ConfigError, "Newly adding a `REQUIRED` column is not allowed #{msg}"
          end
        when 'NULLABLE'
          raise ConfigError, "`NULLABLE` column can not be changed #{msg}"
        when 'REQUIRED'
          if target_mode == 'REPEATED'
            raise ConfigError, "`REQUIRED` column can not be changed to `REPEATED` #{msg}"
          end
        when 'REPEATED'
          raise ConfigError, "`REPEATED` column can not be changed #{msg}"
        end
      end

      def normalize_columns(columns)
        columns = shallow_normalize_columns(columns)
        columns.map do |column|
          if column[:type] == 'RECORD' and column[:fields]
            column[:fields] = normalize_columns(column[:fields])
          end
          column
        end
      end

      def shallow_normalize_columns(columns)
        columns.map {|column| shallow_normalize_column(column) }
      end

      def shallow_normalize_columns!(columns)
        columns.each {|column| shallow_normalize_column!(column) }
        columns
      end

      def shallow_normalize_column(column)
        shallow_normalize_column!(column.dup)
      end

      def shallow_normalize_column!(column)
        symbolize_keys!(column)
        column[:type] = column[:type].upcase if column[:type]
        column[:mode] ||= 'NULLABLE'
        column[:mode] = column[:mode].upcase
        column
      end

      def symbolize_keys!(column)
        new_column = column.map do |key, val|
          [key.to_sym, val]
        end.to_h
        column.replace(new_column)
      end

      # @param [Array] columns
      # [{
      #   name: 'citiesLived',
      #   type: 'RECORD',
      #   fields: [
      #     {
      #       name: 'place', type: 'RECORD',
      #       fields: [
      #         { name: 'city', type: 'STRING' }, { name: 'postcode', type: 'STRING' }
      #       ]
      #     },
      #     { name: 'yearsLived', type: 'INTEGER' }
      #   ]
      # }]
      # @return Hash
      # {
      #   'citiesLived.place.city' => {
      #     type: 'STRING'
      #   },
      #   'citiesLived.place.postcode' => {
      #     type: 'STRING'
      #   },
      #   'citiesLived.yearsLived' => {
      #     type: 'INTEGER'
      #   }
      # }
      def flattened_columns(columns, parent_name: nil)
        result = {}
        columns.each do |column|
          column_name = parent_name.nil? ? column[:name] : "#{parent_name}.#{column[:name]}"
          if column[:type].upcase != 'RECORD'
            result[column_name] = {}.tap do |value|
              value[:type] = column[:type]
              value[:mode] = column[:mode] if column[:mode]
            end
          else
            result.merge!(flattened_columns(column[:fields], parent_name: column_name))
          end
        end
        result
      end

      def equals?(source_columns, target_columns)
        diff_columns(source_columns, target_columns).empty? and \
          diff_columns(target_columns, source_columns).empty?
      end

      # target_columns - source_columns
      def diff_columns(source_columns, target_columns)
        _target_columns = shallow_normalize_columns(target_columns)
        _source_columns = shallow_normalize_columns(source_columns)
        diff_columns = _target_columns - _source_columns # shallow diff

        diff_columns.map do |target_column|
          t = target_column
          source_column = find_column_by_name(_source_columns, target_column[:name])
          next t unless source_column
          next t unless target_column[:type] == 'RECORD' and source_column[:type] == 'RECORD'
          next t unless target_column[:fields] and source_column[:fields]
          # recusive diff for RECORD columns
          diff_fields = diff_columns(source_column[:fields], target_column[:fields])
          next nil if diff_fields.empty? # remove
          target_column[:fields] = diff_fields
          target_column
        end.compact
      end

      # diff with only column_names
      # target_columns - source_columns
      def diff_columns_by_name(source_columns, target_columns)
        _target_columns = shallow_normalize_columns(target_columns)
        _source_columns = shallow_normalize_columns(source_columns)
        diff_columns = _target_columns - _source_columns # shallow diff

        diff_columns.map do |target_column|
          t = target_column
          source_column = find_column_by_name(_source_columns, target_column[:name])
          next t unless source_column
          next nil unless target_column[:type] == 'RECORD' and source_column[:type] == 'RECORD'
          next nil unless target_column[:fields] and source_column[:fields]
          # recusive diff for RECORD columns
          diff_fields = diff_columns_by_name(source_column[:fields], target_column[:fields])
          next nil if diff_fields.empty? # remove
          target_column[:fields] = diff_fields
          target_column
        end.compact
      end

      # 1. target_column[:mode] ||= source_column[:mode] || 'NULLABLE' (not overwrite, but set if does not exist)
      # 2. Add into target_columns if a source column does not exist in target_columns
      #
      # @param [Array] source_columns
      # @param [Array] target_columns
      def reverse_merge!(source_columns, target_columns)
        shallow_normalize_columns!(source_columns)
        shallow_normalize_columns!(target_columns)

        source_columns.map do |source_column|
          if target_column = find_column_by_name(target_columns, source_column[:name])
            target_column[:mode] ||= source_column[:mode] || 'NULLABLE'
            target_column[:type] ||= source_column[:type] # should never be happened
            # Recursive merge fields of `RECORD` type
            if target_column[:type] == 'RECORD' and target_column[:fields] and source_column[:fields]
              reverse_merge!(source_column[:fields], target_column[:fields])
            end
          else
            target_column = source_column.dup
            target_column[:mode] ||= 'NULLABLE'
            target_columns << target_column
          end
        end
        target_columns
      end

      def reject_columns!(drop_columns, target_columns)
        flattened_drop_columns = flattened_columns(drop_columns)

        flattened_drop_columns.keys.each do |flattened_name|
          # paths like a %w(citiesLived place city child1)
          paths = flattened_name.split('.')
          # object_id of fields and target_columns are different.
          # But the internal elements refer to the same ones
          fields = target_columns
          paths.each do |path|
            # The last element of the path does not have the fields
            next if path == paths.last
            # find recursively
            column = fields.find { |f| f[:name] == path }
            next if column.nil?
            fields = column[:fields]
          end

          unless fields.empty?
            fields.delete_if { |f| f[:name] == paths.last }
          end
        end
        target_columns
      end

      def build_query_fields(source_columns, target_columns)
        flattened_source_columns = flattened_columns(source_columns)
        flattened_target_columns = flattened_columns(target_columns)

        query_fields = flattened_target_columns.map do |flattened_name, flattened_target_column|
          flattened_source_column = flattened_source_columns[flattened_name]
          target_type = flattened_target_column[:type].upcase

          if flattened_source_column
            "#{target_type}(#{flattened_name}) AS #{flattened_name}"
          else
            flattened_name
            #  MEMO: NULL cast like "#{target_type}(NULL) AS #{flattened_name}" breaks RECORD columns as
            #  INTEGER(NULL) AS add_record.add_record.add_column1 => add_record_add_record_add_column1
            #  We have to add columns with patch_table beforehand
          end
        end
      end

      def make_nullable!(columns)
        columns.each do |column|
          if column[:fields]
            make_nullable!(column[:fields])
          else
            column[:mode] = 'NULLABLE'
          end
        end
        columns
      end
    end
  end
end
