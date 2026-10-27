# dbt-databricks-airflow-cdc-pipeline
dbt-driven analytics pipeline on Databricks. Ingests Postgres changes (CDC) and AWS S3 files incrementally, builds a one-big-table with data quality checks, and models it into a star schema with dbt. Orchestrated end to end with Apache Airflow
