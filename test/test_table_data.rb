require_relative 'helper.rb'
require 'bigquery_migration/table_data'

class BigqueryMigration
  class TestTableData < Test::Unit::TestCase
    sub_test_case "generate_table_rows" do
      def test_generate_table_rows_simple
        columns = [
          { name: 'string', type: 'STRING', mode: 'NULLABLE'},
          { name: 'integer', type: 'INTEGER', mode: 'NULLABLE'},
          { name: 'float', type: 'FLOAT', mode: 'NULLABLE'},
          { name: 'boolean', type: 'BOOLEAN', mode: 'NULLABLE'},
          { name: 'timestamp', type: 'TIMESTAMP', mode: 'NULLABLE'},
        ]

        rows = [
          { f: [
            {v: "foo"},
            {v: "1"},
            {v: "1.1"},
            {v: "true"},
            {v: "1.444435200E9"}
          ] },
          { f: [
            {v: "bar"},
            {v: "2"},
            {v: "2.2"},
            {v: "false"},
            {v: "1.444435200E9"}
          ] }
        ]

        expected = [
          [ "foo", "1", "1.1", "true", "1.444435200E9" ],
          [ "bar", "2", "2.2", "false", "1.444435200E9" ]
        ]

        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end

      def test_generate_table_rows_repeated_and_record_simple
        columns = [
          { name: 'repeated_record', type: 'RECORD', mode: 'REPEATED', fields: [
            { name: 'record', type: 'RECORD', mode: 'NULLABLE', fields: [
              { name: 'repeated_time', type: 'TIMESTAMP', mode: 'REPEATED' }
            ] },
          ] }
        ]

        rows = [
          { f: [
            { v: [
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: [
                        {v: "1.444435200E9"},
                        {v: "1.444435200E9"}
                      ] }
                    ] }
                  }
                ] }
              },
              v: {
                f: [
                  { v:
                    { f: [
                      { v: [
                        {v: "1.444435200E9"},
                        {v: "1.444435200E9"},
                        {v: "1.444435200E9"}
                      ] }
                    ] }
                  }
                ]
              }
            ] }
          ] }
        ]

        expected = [
          # only single row
          [
            ["1.444435200E9"],
            ["1.444435200E9"],
            ["1.444435200E9"],
            ["1.444435200E9"],
            ["1.444435200E9"]
          ]
        ]

        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end

      def test_generate_table_rows_repeated_and_record_multiple
        columns = [
          { name: 'repeated_record', type: 'RECORD', mode: 'REPEATED', fields: [
            { name: 'record', type: 'RECORD', mode: 'NULLABLE', fields: [
              { name: 'repeated_time', type: 'TIMESTAMP', mode: 'REPEATED' }
            ] },
          ] }
        ]

        rows = [
          { f: [
            { v: [
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: [
                        {v: "1.444435200E9"},
                        {v: "1.444435200E9"}
                      ] }
                    ] }
                  }
                ] }
              }
            ] }
          ] },
          { f: [
            { v: [
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: [
                        {v: "1.444435200E9"},
                        {v: "1.444435200E9"}
                      ] }
                    ] }
                  }
                ] }
              }
            ] }
          ] }
        ]

        expected = [
          # first row
          [
            ["1.444435200E9"],
            ["1.444435200E9"],
          ],
          # second row
          [
            ["1.444435200E9"],
            ["1.444435200E9"],
          ]
        ]

        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end

      def test_generate_table_rows_repeated_in_middle_row
        columns = [
          { "name": "string", "type": "STRING", "mode": "NULLABLE" },
          { "name": "integer", "type": "INTEGER", "mode": "NULLABLE" },
          { "name": "repeated", "type": "STRING", "mode": "REPEATED" },
          { "name": "float", "type": "FLOAT", "mode": "NULLABLE" },
          { "name": "boolean", "type": "BOOLEAN", "mode": "NULLABLE" },
          { "name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE" }
        ]

        rows = [
          { f: [
          { v: "foo" },
          { v: "1" },
          { v: [] },
          { v: "1.1" },
          { v: "true" },
          { v: "1.444435200E9" }
         ] },
        { f: [
          { v: "foo" },
          { v: "3" },
          { v: [] },
          { v: "3.3" },
          { v: "true" },
          { v: "1.444435200E9" }
         ] },
        { f: [
          { v: "foo" },
          { v: "4" },
          { v: [] },
          { v: "4.4" },
          { v: "false" },
          { v: "1.444435200E9" }
         ] },
        { f: [
          { v: "foo" },
          { v: "2" },
          { v: [
            { v: "foo" },
            { v: "bar" }
           ] },
          { v: "2.2" },
          { v: "false" },
          { v: "1.444435200E9" }
         ] }
        ]

        expected = [
          # first row
          [
            [ "foo", "1", nil, "1.1", "true", "1.444435200E9" ]
          ],
          # second row
          [
            [ "foo", "3", nil, "3.3", "true", "1.444435200E9" ],
          ],
          # third row
          [
            [ "foo", "4", nil, "4.4", "false", "1.444435200E9" ]
          ],
          # fourth row
          [
            [ "foo", "2", "foo", "2.2", "false", "1.444435200E9" ],
            [ nil, nil, "bar", nil, nil, nil ],
          ],
        ]


        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end

      def test_generate_table_rows_repeated_and_record_in_middle_row
        columns = [
          { "name": "string", "type": "STRING", "mode": "NULLABLE" },
          { "name": "integer", "type": "INTEGER", "mode": "NULLABLE" },
          { "name": "repeated", "type": "RECORD", "mode": "REPEATED", "fields": [
            { "name": "record", "type": "STRING", "mode": "REPEATED" }
          ] },
          { "name": "float", "type": "FLOAT", "mode": "NULLABLE" },
          { "name": "boolean", "type": "BOOLEAN", "mode": "NULLABLE" },
          { "name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE" }
        ]

        rows = [
          { f: [
            { v: "foo" },
            { v: "1" },
            { v: [] },
            { v: "1.1" },
            { v: "true" },
            { v: "1.444435200E9" }
          ] },
          { f: [
            { v: "foo" },
            { v: "4" },
            { v: [] },
            { v: "4.4" },
            { v: "true" },
            { v: "1.444435200E9" }
          ] },
          { f: [
            { v: "foo" },
            { v: "5" },
            { v: [] },
            { v: "5.5" },
            { v: "false" },
            { v: "1.444435200E9" }
          ] },
          { f: [
            { v: "foo" },
            { v: "2" },
            { v: [
              { v:
                { f: [
                  { v: [
                    { v: "foo" },
                    { v: "bar" }
                  ] }
                ] }
              },
              { v:
                { f: [
                  { v: [
                    { v: "foo" },
                    { v: "bar" }
                  ] }
                ] }
              }
            ] },
            { v: "2.2" },
            { v: "false" },
            { v: "1.444435200E9" }
          ] },
          { f: [
            { v: "foo" },
            { v: "3" },
            { v: [
              { v:
                { f: [
                  { v: [
                    { v: "foo" },
                    { v: "bar" }
                  ] }
                ] }
              },
              { v:
                { f: [
                  { v: [
                      { v: "foo" },
                      { v: "bar" }
                    ] }
                ] }
              }
            ] },
            { v: "3.3" },
            { v: "false" },
            { v: "1.444435200E9" }
          ] }
        ]

        expected = [
          # first row
          [
            [ "foo", "1", nil, "1.1", "true", "1.444435200E9" ]
          ],
          # second row
          [
            [ "foo", "4", nil, "4.4", "true", "1.444435200E9" ]
          ],
          # third row
          [
            [ "foo", "5", nil, "5.5", "false", "1.444435200E9" ]
          ],
          # fourth row
          [
            [ "foo", "2", "foo", "2.2", "false", "1.444435200E9" ],
            [ nil, nil, "bar", nil, nil, nil ],
            [ nil, nil, "foo", nil, nil, nil ],
            [ nil, nil, "bar", nil, nil, nil ]
          ],
          # fifth row
          [
            [ "foo", "3", "foo", "3.3", "false", "1.444435200E9" ],
            [ nil, nil, "bar", nil, nil, nil ],
            [ nil, nil, "foo", nil, nil, nil ],
            [ nil, nil, "bar", nil, nil, nil ]
          ],
        ]

        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end

      def test_generate_table_rows_repeated_and_record_complex
        columns = [
          { name: 'repeated_record', type: 'RECORD', mode: 'REPEATED', fields: [
            { name: 'record', type: 'RECORD', mode: 'NULLABLE', fields: [
              { name: 'child', type: 'STRING', mode: 'NULLABLE' },
              { name: 'repeated_time', type: 'TIMESTAMP', mode: 'REPEATED' }
            ] },
            { name: 'repeated_time', type: 'TIMESTAMP', mode: 'REPEATED' }
          ] },
          { name: 'repeated_string', type: 'STRING', mode: 'REPEATED' },
          { name: 'repeated_int', type: 'INTEGER', mode: 'REPEATED' },
          { name: 'repeated_record2', type: 'RECORD', mode: 'REPEATED', fields: [
            { name: 'record2', type: 'RECORD', mode: 'NULLABLE', fields: [
              { name: 'repeated_float', type: 'FLOAT', mode: 'REPEATED' },
              { name: 'child2', type: 'STRING', mode: 'REQUIRED' }
            ] }
          ] }
        ]

        rows = [
          { f: [
            { v: [
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: "foo"},
                      { v: [
                        { v: "1.44423E9"},
                        { v: "1.4443164E9"}
                      ] }
                    ] }
                  },
                  { v: [
                    { v: "1.4444028E9"},
                    { v: "1.4444028E9"}
                  ] }
                ] }
              },
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: "fuga"},
                      { v: [] }
                    ] }
                  },
                  { v: [
                    { v: "1.4445756E9"},
                    { v: "1.444662E9"}
                  ] }
                ] }
              }
            ] },
            { v: [
              { v: "one"},
              { v: "two"},
              { v: "three"}
            ] },
            { v: [
              { v: "1"},
              { v: "2"}
            ] },
            { v: [
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: [
                        { v: "1.1"},
                        { v: "2.2"},
                        { v: "3.3"}
                      ] },
                      { v: "foo2"}
                    ] }
                  }
                ] }
              },
              { v:
                { f: [
                  { v:
                    { f: [
                      { v: [
                        { v: "4.4"},
                        { v: "5.5"},
                        { v: "6.6"},
                        { v: "7.7"}
                      ] },
                      { v: "bar"}
                    ] }
                  }
                ] }
              }
            ] }
          ] }
        ]

        expected = [
          # only single row
          [
            ["foo", "1.44423E9", "1.4444028E9", "one", "1", "1.1", "foo2"],
            [nil, "1.4443164E9", "1.4444028E9", "two", "2", "2.2", nil],
            ["fuga", nil, "1.4445756E9", "three", nil, "3.3", nil],
            [nil, nil, "1.444662E9", nil, nil, "4.4", "bar"],
            [nil, nil, nil, nil, nil, "5.5", nil],
            [nil, nil, nil, nil, nil, "6.6", nil],
            [nil, nil, nil, nil, nil, "7.7", nil]
          ]
        ]

        assert { TableData.new(columns, rows).generate_table_rows == expected }
      end
    end
  end
end
