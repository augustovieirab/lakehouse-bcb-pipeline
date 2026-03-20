run_ingestion:
	python ingestion/raw_to_bronze.py
	python ingestion/bronze_to_silver.py

run_dbt:
	cd dbt_project && dbt run

run_all:
	make run_ingestion
	make run_dbt