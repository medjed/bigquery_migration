# BigqueryMigration

BigqueryMigraiton is a tool or a ruby library to migrate (or alter) BigQuery table schema.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'bigquery_migration'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install bigquery_migration

## Usage

Define your desired schema, this tool automatically detects differences with the target table, and takes care of adding columns, or dropping columns (actually, select & copy is issued), or changing types.

### CLI

config.yml

```yaml
bigquery: &bigquery
  json_keyfile: your-project-000.json
  dataset: your_dataset_name
  table: your_table_name

actions:
- action: create_dataset
  <<: *bigquery
- action: migrate_table
  <<: *bigquery
  columns:
    - { name: 'timestamp', type: 'TIMESTAMP' }
    - name: 'record'
      type: 'RECORD'
      fields:
        - { name: 'string', type: 'STRING' }
        - { name: 'integer', type: 'INTEGER' }
```

Run

```
$ bundle exec bq_migrate run config.yml # dry-run
$ bundle exec bq_migrate run config.yml --exec
```

### Library

```ruby
require 'bigquery_migration'

config = {
  json_keyfile: '/path/to/your-project-000.json'
  dataset: 'your_dataset_name'
  table: 'your_table_name'
}
columns = [
  { name: 'string', type: 'STRING' },
  { name: 'record', type: 'RECORD', fields: [
    { name: 'integer', type: 'INTEGER' },
    { name: 'timestamp', type: 'TIMESTAMP' },
  ] }
]

migrator = BigqueryMigration.new(config)
migrator.migrate_table(columns: columns)
# migrator.migrate_table(schema_file: '/path/to/schema.json')
```

## LIMITATIONS

There are serveral limitations because of BigQuery API limitations:

* Can not handle `mode: REPEATED` columns
* Can add only `mode: NULLABLE` columns
* Columns become `mode: NULLABLE` after type changing
* Will be charged because a query is issued (If only adding columns, it is not charged because it uses patch_table API)

This tool has an advantage that it is **faster** than reloading data entirely.

## Further Details

* See [BigQueryテーブルのスキーマを変更する - sonots:blog](http://blog.livedoor.jp/sonots/archives/47294596.html) (Japanese)

## Development

### Run example:

**Service Account**

Prepare your service account json at `example/your-project-000.json`, then

```
$ bundle exec bq_migrate run example/example.yml # dry-run
$ bundle exec bq_migrate run example/example.yml --exec
```

**OAuth**

Install gcloud into your development environment:

```
curl https://sdk.cloud.google.com | bash
gcloud init
gcloud auth login
gcloud auth application-default login
gcloud config set project <GCP_PROJECT_NAME>
```

Make sure `gcloud` works

```
gcloud compute instances list
```

Run as:

```
$ bundle exec bq_migrate run example/application_default.yml # dry-run
$ bundle exec bq_migrate run example/application_default.yml --exec
```

### Run test:

```
$ bundle exec rake test
```

To run tests which directly connects to BigQuery, prepare `example/your-project-000.json`, then

```
$ bundle exec rake test
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/sonots/bigquery_migration. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
