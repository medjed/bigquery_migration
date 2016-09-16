# 0.1.7 (2016/09/16)

Fixes:

* migrate_table should create table only if a table does not exist

Enhancements:

* create_table should not create a table with empty columns

# 0.1.6 (2016/07/26)

Fixes:

* Fix empty hash to nil

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
