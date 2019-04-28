# 0.3.2 (2019/04/29)

Enhancements:

* Support clustered table

# 0.3.1 (2018/05/23)

Enhancements:

* Support newly added location option of google-api-ruby-client.

# 0.3.0 (2017/04/26)

Enhancements:

* Support more authentication methods such as oauth, compute_engine, application_default

# 0.2.2 (2017/04/04)

Enhancements:

* Support google-api-ruby-client >= v0.11.0

# 0.2.1 (2017/03/31)

Enhancements:

* Accept DATE, DATETIME, TIME as column types

# 0.2.0 (2016/10/03)

Enhancements:

* Support migrate_partitioned_table

Fixes:

* Fix list_table_data for when a value is an empty hash

# 0.1.7 (2016/09/17)

Fixes:

* Prohibit to create a table with empty columns
* Create a table only if a table does not exist

# 0.1.6 (2016/07/26)

Fixes:

* Fix empty hash to nil for list table data

# 0.1.5 (2016/07/25)

Enhancements:

* Support record type and repeated mode for list table data

# 0.1.4 (2016/07/12)

Fixes:

* Fix to allow downcase type and mode

# 0.1.3 (2016/04/22)

Enhancements:

* Support new BYTES types
* Add exe/bq-migrate as an alias to exe/bq_migrate

# 0.1.2 (2016/04/14)

Changes:

* Genearate job_id on client side as [google recommends](https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#managingjobs)

# 0.1.1 (2016/04/12)

Changes:

* Expose wait_load method

# 0.1.0 (2016/04/08)

Initial release
