require_relative 'helper.rb'
require 'bigquery_migration/bigquery_wrapper'

unless File.exist?(JSON_KEYFILE)
  puts "#{JSON_KEYFILE} is not found. Skip test/test_bigquery_wrapper.rb"
else
  class BigqueryMigration
    class TestBigqueryWrapper < Test::Unit::TestCase
      def instance
        @instance ||= BigqueryWrapper.new(config)
      end

      def config
        {
          'json_keyfile' => JSON_KEYFILE,
          'dataset'      => 'bigquery_migration_unittest',
          'table'        => 'test',
        }
      end

      sub_test_case "configure" do
        def test_configure_json_keyfile
          config = {
            'json_keyfile' => JSON_KEYFILE,
            'dataset'      => 'bigquery_migration_unittest',
            'table'        => 'test',
          }
          assert_nothing_raised { instance.project }
          assert_nothing_raised { instance.dataset }
          assert_nothing_raised { instance.table }
          assert_nothing_raised { instance.client }
        end

        def test_configure_json_keyfile_content_json
          config = {
            'json_keyfile' => {
              'content' => File.read(JSON_KEYFILE),
            },
            'dataset'      => 'bigquery_migration_unittest',
            'table'        => 'test',
          }
          assert_nothing_raised { instance.project }
          assert_nothing_raised { instance.dataset }
          assert_nothing_raised { instance.table }
          assert_nothing_raised { instance.client }
        end

        def test_configure_json_keyfile_content_hash
          config = {
            'json_keyfile' => {
              'content' => JSON.parse(File.read(JSON_KEYFILE)),
            },
            'dataset'      => 'bigquery_migration_unittest',
            'table'        => 'test',
          }
          instance = BigqueryWrapper.new(config)
          assert_nothing_raised { instance.project }
          assert_nothing_raised { instance.dataset }
          assert_nothing_raised { instance.table }
          assert_nothing_raised { instance.client }
        end
      end

      def test_create_dataset
        assert_nothing_raised { instance.create_dataset }
        assert_nothing_raised { instance.get_dataset }
      end

      def test_create_table
        instance.drop_table rescue nil
        columns = [
          { name: 'column1', type: 'INTEGER' },
          { name: 'column2', type: 'STRING' },
          { name: 'column3', type: 'FLOAT' },
          { name: 't',       type: 'TIMESTAMP' },
          { name: 'record',  type: 'RECORD', fields:[
            { name: 'column4', type: 'STRING' },
            { name: 'column5', type: 'INTEGER' },
          ]},
        ]
        assert_nothing_raised { instance.create_table(columns: columns) }
        assert_nothing_raised { instance.get_table }
      end

      def test_drop_table
        instance.create_table(columns: [{ name: 'column1', type: 'INTEGER' }])
        assert_nothing_raised { instance.drop_table }
        assert_raise(NotFoundError) { instance.get_table }
      end

      def test_list_tables
        instance.create_table(table: 'table1', columns: [{ name: 'column1', type: 'INTEGER' }])
        instance.create_table(table: 'table2', columns: [{ name: 'column1', type: 'INTEGER' }])
        result = instance.list_tables
        assert { result[:tables] == ['table1', 'table2'] }
        instance.drop_table(table: 'table1')
        instance.drop_table(table: 'table2')
      end

      sub_test_case "purge_tables" do
        def before_tables
          %w[
            test_20160301
            test_20160301_00
            test_20160229
            test_20160229_23
            test_20160229_22
            test_20160228
            test_23_20160229
            test_22_20160229
            test_00_20160301
          ]
        end

        def test_purge_tables_daily
          stub(instance).list_tables { { tables: before_tables } }
          result = instance.purge_tables(
            table_prefix: 'test_', suffix_format: '%Y%m%d', purge_before: '20160229'
          )
          expected = %w[test_20160229 test_20160228]
          assert { result[:delete_tables] == expected }
        end

        def test_purge_tables_hourly_1
          stub(instance).list_tables { { tables: before_tables } }
          result = instance.purge_tables(
            table_prefix: 'test_', suffix_format: '%Y%m%d_%H', purge_before: '20160229_23'
          )
          expected = %w[test_20160229_23 test_20160229_22]
          assert { result[:delete_tables] == expected }
        end

        def test_purge_tables_hourly_2
          stub(instance).list_tables { { tables: before_tables } }
          result = instance.purge_tables(
            table_prefix: 'test_', suffix_format: '%H_%Y%m%d', purge_before: '23_20160229'
          )
          expected = %w[test_23_20160229 test_22_20160229]
          assert { result[:delete_tables] == expected }
        end
      end

      sub_test_case "table_data" do
        def setup
          instance.drop_table
        end

        def teardown
          instance.drop_table
        end

        # Streaming insert takes time to be reflected. Let me coment out....
=begin
        def test_insert_all_and_list_table_data
          instance.create_table(columns: [
            { 'name' => 'id', 'type' => 'INTEGER' },
            { 'name' => 'string', 'type' => 'STRING', 'mode' => 'REQUIRED' },
            { 'name' => 'record', 'type' => 'RECORD', 'fields' => [
              { 'name' => 'child1', 'type' => 'STRING' },
            ] },
            { 'name' => 'null', 'type' => 'STRING' },
          ])

          assert_nothing_raised do
            instance.insert_all_table_data(rows: [
              {'id' => 1, 'string' => 'foo', 'record' => { 'child1' => 'foo' }},
              {'id' => 2, 'string' => 'bar', 'record' => { 'child1' => 'bar' }},
            ])
          end

          result = {}
          assert_nothing_raised { result = instance.list_table_data }
          60.times do
            break if result[:values]
            sleep 1
            result = instance.list_table_data
          end

          expected = {
            total_rows: 2,
            columns: [
              { name: 'id', type: 'INTEGER', mode: 'NULLABLE' },
              { name: 'string', type: 'STRING', mode: 'REQUIRED' },
              { name: 'record.child1', type: 'STRING', mode: 'NULLABLE' },
              { name: 'null', type: 'STRING', mode: 'NULLABLE' },
            ],
            values: [
              ['2','bar','bar',nil],
              ['1','foo','foo',nil],
            ]
          }
          assert { result[:columns] == expected[:columns] }
          assert { result[:values] == expected[:values] }
          # total_rows is not reflected by streming insert ....
          # assert { result[:total_rows] == expected[:total_rows] }
        end
=end
      end

      sub_test_case "patch_table" do
        def setup
          instance.drop_table
        end

        def teardown
          instance.drop_table
        end

        def test_add_columns
          before_columns = [
            { 'name' => 'id', 'type' => 'INTEGER' },
            { 'name' => 'string', 'type' => 'STRING', 'mode' => 'REQUIRED' },
            { 'name' => 'record', 'type' => 'RECORD', 'fields' => [
              { 'name' => 'child1', 'type' => 'STRING' },
            ] },
          ]
          instance.create_table(columns: before_columns)

          add_columns = [
            {"name"=>"new_nullable_column", "type"=>"STRING", "mode"=>"NULLABLE"},
            {"name"=>"new_repeated_column", "type"=>"STRING", "mode"=>"REPEATED"},
            {"name"=>"new_record", "type"=>"RECORD", "fields"=>[
              {"name"=>"new_record_child2", "type"=>"RECORD", "fields"=>[
                {"name"=>"new_record_child3", "type"=>"STRING"}
              ]}
            ]}
          ]
          expected = before_columns + add_columns

          result = instance.patch_table(add_columns: add_columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_mode_change
          before_columns = [
            {"name"=>"id", "type"=>"INTEGER"},
            {"name"=>"record", "type"=>"RECORD", "fields"=> [
              {"name"=>"record", "type"=>"RECORD", "fields"=> [
                {"name"=>"mode_change", "type"=>"STRING", "mode"=>"REQUIRED"}
              ]}
            ]}
          ]
          instance.create_table(columns: before_columns)

          add_columns = [
            {"name"=>"record", "type"=>"RECORD", "fields"=> [
              {"name"=>"record", "type"=>"RECORD", "fields"=> [
                {"name"=>"mode_change", "type"=>"STRING", "mode"=>"NULLABLE"}
              ]}
            ]}
          ]

          expected = [
            {"name"=>"id", "type"=>"INTEGER"},
            {"name"=>"record", "type"=>"RECORD", "fields"=> [
              {"name"=>"record", "type"=>"RECORD", "fields"=> [
                {"name"=>"mode_change", "type"=>"STRING", "mode"=>"NULLABLE"}
              ]}
            ]}
          ]

          result = instance.patch_table(add_columns: add_columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end
      end

      sub_test_case "insert_select" do
        def setup
          instance.drop_table
        end

        def teardown
          instance.drop_table
        end

        def test_insert_select
          columns = [{ 'name' => 'id', 'type' => 'INTEGER' }]
          instance.create_table(columns: columns)

          query = "SELECT id FROM [#{config['dataset']}.#{config['table']}]"
          assert_nothing_raised do
            instance.insert_select(destination_table: 'insert_table', query: query)
          end
          assert_nothing_raised { instance.get_table(table: 'insert_table') }
        ensure
          instance.drop_table(table: 'insert_table')
        end
      end

      sub_test_case "drop_column" do
        def setup
          instance.drop_table
        end

        def teardown
          instance.drop_table
        end

        def test_drop_column_with_drop_columns
          before_columns = [
            { name: 'drop_column', type: 'INTEGER' },
            { name: 'remained_column', type: 'STRING' },
            { name: 'record', type: 'RECORD', fields:[
              { name: 'drop_column', type: 'STRING' },
              { name: 'remained_column', type: 'STRING' },
            ] }
          ]
          instance.create_table(columns: before_columns)

          drop_columns = [
            { name: 'drop_column', type: 'STRING' },
            { name: 'record', type: 'RECORD', fields:[
              { name: 'drop_column', type: 'STRING' },
            ] },
          ]
          expected = [
            { name: 'remained_column', type: 'STRING' },
            { name: 'record', type: 'RECORD', fields:[
              { name: 'remained_column', type: 'STRING' },
            ] }
          ]
          
          result = instance.drop_column(drop_columns: drop_columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_drop_column_with_columns
          before_columns = [
            { name: 'drop_column', type: 'INTEGER' },
            { name: 'remained_column', type: 'STRING' },
            { name: 'record', type: 'RECORD', fields:[
              { name: 'drop_column', type: 'STRING' },
              { name: 'remained_column', type: 'STRING' },
            ] }
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'remained_column', type: 'STRING' },
            { name: 'record', type: 'RECORD', fields:[
              { name: 'remained_column', type: 'STRING' },
              { name: 'add_column', type: 'STRING' },
            ] },
            { name: 'add_column', type: 'STRING' },
          ]
          expected = columns.dup
          
          result = instance.drop_column(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

      end

      sub_test_case "migrate_table" do
        def setup
          instance.drop_table
        end

        def teardown
          instance.drop_table
        end

        def test_add_columns
          before_columns = [
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'remained_column', type: 'STRING' },
                { name: 'new_column', type: 'INTEGER' },
                { name: 'new_record', type: 'RECORD', fields: [
                  { name: 'new_column', type: 'INTEGER' },
                ] }
              ] }
            ] },
            { name: 'new_column', type: 'INTEGER' },
          ]
          expected = columns.dup

          result = instance.migrate_table(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_drop_columns
          before_columns = [
            { name: 'drop_column', type: 'INTEGER' },
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'drop_column', type: 'STRING' },
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          expected = columns.dup

          result = instance.migrate_table(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_add_drop
          before_columns = [
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'remained_column', type: 'STRING' },
                { name: 'drop_column', type: 'STRING' },
              ] }
            ] },
            { name: 'drop_column', type: 'INTEGER' },
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'remained_column', type: 'STRING' },
                { name: 'add_column', type: 'INTEGER' },
              ] },
            ] },
            { name: 'add_column', type: 'STRING', mode: 'REPEATED' },
            { name: 'add_record', type: 'RECORD', fields: [
              { name: 'add_record', type: 'RECORD', fields: [
                { name: 'add_column1', type: 'STRING' },
                { name: 'add_column2', type: 'INTEGER' },
              ] }
            ]}
          ]
          expected = columns.dup

          result = instance.migrate_table(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_type_change
          before_columns = [
            { name: 'type_change', type: 'STRING' },
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'type_change', type: 'STRING' },
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'type_change', type: 'INTEGER' },
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'type_change', type: 'INTEGER' },
                { name: 'remained_column', type: 'STRING' },
              ] }
            ]}
          ]
          expected = columns.dup

          result = instance.migrate_table(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end

        def test_mode_change
          before_columns = [
            { name: 'mode_change', type: 'STRING', mode: 'REQUIRED' },
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'mode_change', type: 'STRING', mode: 'REQUIRED' },
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          instance.create_table(columns: before_columns)

          columns = [
            { name: 'mode_change', type: 'STRING', mode: 'NULLABLE' },
            { name: 'remained_column', type: 'INTEGER' },
            { name: 'record', type: 'RECORD', fields: [
              { name: 'record', type: 'RECORD', fields: [
                { name: 'mode_change', type: 'STRING', mode: 'NULLABLE' },
                { name: 'remained_column', type: 'STRING' },
              ] }
            ] }
          ]
          expected = columns.dup

          result = instance.migrate_table(columns: columns)
          after_columns = result[:after_columns]

          assert { Schema.diff_columns(expected, after_columns) == [] }
        end
      end
    end
  end
end
