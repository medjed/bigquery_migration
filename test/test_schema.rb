require_relative 'helper.rb'
require 'bigquery_migration/schema'

class BigqueryMigration
  class TestSchema < Test::Unit::TestCase
    def columns
      [
        {name: 'boolean',  type: 'BOOLEAN', mode: 'NULLABLE'},
        {name: 'integer',  type: 'INTEGER'},
        {name: 'float',    type: 'FLOAT'},
        {name: 'string',   type: 'STRING'},
        {name: 'timstamp', type: 'TIMESTAMP'},
        {name: 'record',   type: 'RECORD', fields: [
          {name: 'record',   type: 'RECORD', fields: [
            {name: 'string', type: 'STRING', mode: 'NULLABLE'},
          ]},
        ]}
      ]
    end

    sub_test_case "find_column_by_name" do
      def test_find_column_by_name
        expected = {name: 'boolean', type: 'BOOLEAN', mode: 'NULLABLE'}
        assert { Schema.find_column_by_name(columns, 'boolean') == expected }
        assert { Schema.new(columns).find_column_by_name('boolean') == expected }
      end
    end

    sub_test_case "validate_columns!" do
      def test_validate_columns_with_valid
        assert_nothing_raised { Schema.new(columns).validate_columns! }
        assert_nothing_raised { Schema.validate_columns!(columns) }
      end

      def test_validate_columns_with_invalid
        no_name = [{}]
        assert_raise { Schema.validate_columns!(no_name) }

        invalid_name = [{name: '%&%&^**'}]
        assert_raise { Schema.validate_columns!(invalid_name) }

        long_name = [{name: 'a'*129}]
        assert_raise { Schema.validate_columns!(long_name) }

        no_type = [{name: 'name'}]
        assert_raise { Schema.validate_columns!(no_type) }

        invalid_type = [{name: 'name', type: 'foobar'}]
        assert_raise { Schema.validate_columns!(invalid_type) }

        no_mode = [{name: 'name', type: 'STRING'}]
        assert_nothing_raised { Schema.validate_columns!(no_mode) }

        no_mode = [{name: 'name', type: 'STRING'}]
        assert_nothing_raised { Schema.validate_columns!(no_mode) }

        invalid_mode = [{name: 'name', type: 'STRING', mode: 'foobar'}]
        assert_nothing_raised { Schema.validate_columns!(no_mode) }
      end
    end

    sub_test_case "normalize_columns" do
      def test_normalize_columns
        downcase_columns = [
          {name: 'boolean',  type: 'boolean', mode: 'nullable'},
          {name: 'integer',  type: 'integer'},
          {name: 'float',    type: 'float'},
          {name: 'string',   type: 'string'},
          {name: 'timstamp', type: 'timestamp'},
          {name: 'record',   type: 'record', fields: [
            {name: 'record',   type: 'record', fields: [
              {name: 'string', type: 'string', mode: 'nullable'},
            ]},
          ]}
        ]
        expected = [
          {name: 'boolean',  type: 'BOOLEAN',   mode: 'NULLABLE'},
          {name: 'integer',  type: 'INTEGER',   mode: 'NULLABLE'},
          {name: 'float',    type: 'FLOAT',     mode: 'NULLABLE'},
          {name: 'string',   type: 'STRING',    mode: 'NULLABLE'},
          {name: 'timstamp', type: 'TIMESTAMP', mode: 'NULLABLE'},
          {name: 'record',   type: 'RECORD',    mode: 'NULLABLE', fields: [
            {name: 'record',   type: 'RECORD',    mode: 'NULLABLE', fields: [
              {name: 'string', type: 'STRING',      mode: 'NULLABLE'},
            ]},
          ]}
        ]
        result = Schema.normalize_columns(downcase_columns)
        assert { result == expected }
        result = Schema.new(downcase_columns).normalize_columns
        assert { result == expected }
      end
    end

    sub_test_case "flattened_columns" do
      def test_flattened_columns
        columns = [
          { name: 'id', type: 'INTEGER' },
          { name: 'citiesLived', type: 'RECORD', fields: [
            { name: 'place', type: 'RECORD', fields: [
              { name: 'city', type: 'STRING' },
              { name: 'postcode', type: 'STRING' }
            ] },
            { name: 'yearsLived', type: 'INTEGER' }
          ] }
        ]

        expected = {
          'id' => { type: 'INTEGER' },
          'citiesLived.place.city' => { type: 'STRING' },
          'citiesLived.place.postcode' => { type: 'STRING' },
          'citiesLived.yearsLived' => { type: 'INTEGER' }
        }
        result = Schema.flattened_columns(columns)
        assert { result == expected }
      end
    end

    sub_test_case "diff_columns" do
      sub_test_case "without intersect" do
        def subset
          [
            {:name=>"remained_column", :type=>"INTEGER"},
            {:name=>"record",
             :type=>"RECORD",
             :fields=>[
               {:name=>"record", :type=>"RECORD", :fields=>[
                 {:name=>"remained_column", :type=>"STRING"}
               ]}
             ]}
          ]
        end

        def superset
          [
            {:name=>"remained_column", :type=>"INTEGER"},
            {:name=>"record", :type=>"RECORD", :fields=>[
              {:name=>"record", :type=>"RECORD", :fields=>[
                {:name=>"remained_column", :type=>"STRING"},
                {:name=>"new_column", :type=>"INTEGER"}
              ]},
              {:name=>"new_record", :type=>"RECORD", :fields=>[
                {:name=>"new_column", :type=>"INTEGER"}
              ]}
            ]},
            {:name=>"new_required_column", :type=>"INTEGER", :mode=>"REQUIRED"}
          ]
        end

        def test_diff_columns_subset
          result = Schema.new(subset).diff_columns(superset)
          assert { result == [] }
        end

        def test_diff_columns_superset
          expected = [
            {:name=>"record", :type=>"RECORD", :fields=>[
              {:name=>"record", :type=>"RECORD", :fields=>[
                {:name=>"new_column", :type=>"INTEGER", :mode=>"NULLABLE" }
              ], :mode=>"NULLABLE"},
              {:name=>"new_record", :type=>"RECORD", :fields=>[
                {"name"=>"new_column", "type"=>"INTEGER", :mode=>"NULLABLE" }
              ], :mode=>"NULLABLE"}
            ], :mode=>"NULLABLE"},
            {:name=>"new_required_column", :type=>"INTEGER", :mode=>"REQUIRED"}
          ]
          result = Schema.new(superset).diff_columns(subset)
          assert { Schema.equals?(result, expected) }
        end
      end

      sub_test_case "with intersect" do
        def before_columns
          [
            {"name"=>"drop_column", "type"=>"INTEGER"},
            {"name"=>"remained_column", "type"=>"INTEGER"},
            {"name"=>"record", "type"=>"RECORD", "fields"=>[
              {"name"=>"record", "type"=>"RECORD", "fields"=>[
                {"name"=>"drop_column", "type"=>"INTEGER"},
                {"name"=>"remained_column", "type"=>"STRING"}
              ]}
            ]}
          ]
        end

        def after_columns
          [
            {"name"=>"remained_column", "type"=>"INTEGER"},
            {"name"=>"record", "type"=>"RECORD", "fields"=>[
              {"name"=>"record", "type"=>"RECORD", "fields"=>[
                {"name"=>"remained_column", "type"=>"STRING"},
                {"name"=>"new_column", "type"=>"INTEGER"}
              ]},
              {"name"=>"new_record", "type"=>"RECORD", "fields"=>[
                {:name=>"new_column", :type=>"INTEGER"}
              ]}
            ]},
            {"name"=>"new_required_column", "type"=>"INTEGER", "mode"=>"REQUIRED"}
          ]
        end

        def test_diff_columns_drop_columns
          drop_columns = Schema.new(before_columns).diff_columns(after_columns)
          expected = [
            {:name=>"drop_column", :type=>"INTEGER", :mode=>"NULLABLE"},
            {:name=>"record", :type=>"RECORD", :mode=>"NULLABLE", :fields=>[
              {:name=>"record", :type=>"RECORD", :mode=>"NULLABLE", :fields=>[
                {:name=>"drop_column", :type=>"INTEGER", :mode=>"NULLABLE" }
              ]}
            ]}
          ]
          assert { Schema.equals?(drop_columns, expected) }
        end

        def test_diff_columns_add_columns
          add_columns = Schema.new(after_columns).diff_columns(before_columns)
          expected = [
            {:name=>"record", :type=>"RECORD", :mode=>"NULLABLE", :fields=>[
              {:name=>"record", :type=>"RECORD", :mode=>"NULLABLE", :fields=>[
                {:name=>"new_column", :type=>"INTEGER", :mode=>"NULLABLE"}
              ]},
              {:name=>"new_record", :type=>"RECORD", :mode=>"NULLABLE", :fields=>[
                {"name"=>"new_column", "type"=>"INTEGER", :mode=>"NULLABLE"}
              ]}
            ]},
            {:name=>"new_required_column", :type=>"INTEGER", :mode=>"REQUIRED"}
          ]
          assert { Schema.equals?(add_columns, expected) }
        end
      end
    end

    sub_test_case "diff_columns_by_name" do
      def before_columns
        [
          {:name=>"drop_column", :type=>"INTEGER"},
          {:name=>"record", :type=>"RECORD", :fields=>[
            {:name=>"record", :type=>"RECORD", :fields=>[
              {:name=>"drop_column", :type=>"INTEGER"},
            ]}
          ]}
        ]
      end

      def after_columns
        [
          {:name=>"drop_column", :type=>"STRING"},
          {:name=>"record", :type=>"RECORD", :fields=>[
            {:name=>"record", :type=>"RECORD", :fields=>[
              {:name=>"drop_column", :type=>"STRING"},
              {:name=>"new_column", :type=>"INTEGER"}
            ]},
            {:name=>"new_record", :type=>"RECORD", :fields=>[
              {:name=>"new_column", :type=>"INTEGER"}
            ]}
          ]},
          {:name=>"new_required_column", :type=>"INTEGER", :mode=>"REQUIRED"}
        ]
      end

      def test_diff_columns_by_name
        diff_columns = Schema.new(after_columns).diff_columns_by_name(before_columns)
        expected = [
          {:name=>"record", :type=>"RECORD", :fields=>[
            {:name=>"record", :type=>"RECORD", :fields=>[
              {:name=>"new_column", :type=>"INTEGER"}
            ]},
            {:name=>"new_record", :type=>"RECORD", :fields=>[
              {:name=>"new_column", :type=>"INTEGER"}
            ]}
          ]},
          {:name=>"new_required_column", :type=>"INTEGER", :mode=>"REQUIRED"}
        ]

        assert { Schema.equals?(expected, diff_columns) }
      end
    end

    sub_test_case "reverse_merge!" do
      def test_reverse_merge!
        source_columns = [
          { name: 'id', type: 'INTEGER', mode: 'NULLABLE' },
          { name: 'name', type: 'RECORD', mode: 'REQUIRED', fields: [
            { name: 'first_name', type: 'STRING', mode: 'NULLABLE' },
            { name: 'last_name', type: 'STRING' },
            { name: 'new_column', type: 'STRING' },
          ] }
        ]

        target_columns = [
          { name: 'id', type: 'INTEGER' },
          { name: 'name', type: 'RECORD', mode: 'NULLABLE', fields: [
            { name: 'first_name', type: 'STRING' },
            { name: 'last_name', type: 'STRING' },
          ] },
        ]

        expected = [
          { name: 'id', type: 'INTEGER', mode: 'NULLABLE' },
          { name: 'name', type: 'RECORD', mode: 'NULLABLE', fields: [
            { name: 'first_name', type: 'STRING', mode: 'NULLABLE' },
            { name: 'last_name', type: 'STRING', mode: 'NULLABLE' },
            { name: 'new_column', type: 'STRING', mode: 'NULLABLE' },
          ] }
        ]

        result = Schema.new(target_columns).reverse_merge!(source_columns)
        assert { result == expected }
      end
    end

    sub_test_case "reject_columns!" do
      def test_reject_columns!
        target_columns = [
          { name: 'id', type: 'INTEGER' },
          { name: 'citiesLived', type: 'RECORD', fields: [
            { name: 'place', type: 'RECORD', fields: [
              { name: 'city', type: 'RECORD', fields: [
                { name: 'child1', type: 'STRING' },
                { name: 'child2', type: 'STRING' }
              ] },
              { name: 'postcode', type: 'STRING' }
            ] },
            { name: 'yearsLived', type: 'INTEGER' }
          ] }
        ]

        drop_columns = [
          { name: 'citiesLived', type: 'RECORD', fields: [
            { name: 'place', type: 'RECORD', fields: [
              { name: 'city', type: 'RECORD', fields: [
                { name: 'child2', type: 'STRING' },
              ] }
            ] }
          ] }
        ]

        expected = [
          { name: 'id', type: 'INTEGER' },
          { name: 'citiesLived', type: 'RECORD', fields: [
            { name: 'place', type: 'RECORD', fields: [
              { name: 'city', type: 'RECORD', fields: [
                name: 'child1', type: 'STRING'
              ]
              },
              { name: 'postcode', type: 'STRING' }
            ] },
            { name: 'yearsLived', type: 'INTEGER' }
          ] }
        ]

        result = Schema.reject_columns!(drop_columns, target_columns)
        assert { result == expected }
      end
    end

    sub_test_case "build_query_fields" do
      def subset
        subset = [
          {name: "remained_column", type: "INTEGER"},
          {name: "record", type: "RECORD", fields:  [
            {name: "record", type: "RECORD", fields: [
              {name: "remained_column", type: "STRING" }
            ]}
          ]}
        ]
      end

      def superset
        [
          {name: "remained_column", type: "INTEGER"},
          {name: "record", type: "RECORD", fields:  [
            {name: "record", type: "RECORD", fields: [
              {name: "remained_column", type: "STRING" },
              {name: "new_column", type: "INTEGER" }
            ]},
            {name: "new_record", type: "RECORD", fields: [
              {name: "new_column", type: "INTEGER"}
            ]}
          ]},
          {name: "new_required_column", type: "INTEGER", mode: "REQUIRED" }
        ]
      end

      def test_build_query_fields_for_subset
        target_columns = subset
        source_columns = superset

        schema = Schema.new(target_columns)
        result = schema.build_query_fields(source_columns)
        expected = [
          "INTEGER(remained_column) AS remained_column",
          "STRING(record.record.remained_column) AS record.record.remained_column"
        ]
        assert { expected == result }
      end

      def test_build_query_fields_for_superset
        target_columns = superset
        source_columns = subset

        schema = Schema.new(target_columns)
        result = schema.build_query_fields(source_columns)
        expected = [
          "INTEGER(remained_column) AS remained_column",
          "STRING(record.record.remained_column) AS record.record.remained_column",
          "record.record.new_column",
          "record.new_record.new_column",
          "new_required_column"
        ]
        assert { expected == result }
      end
    end
  end
end
