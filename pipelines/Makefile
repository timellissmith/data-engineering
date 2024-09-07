mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
airflow_dir := $(shell echo ${mkfile_path} | sed s/Makefile//g)
export root_dir := ${airflow_dir}/../
export LOCAL_DAGS_FOLDER=${airflow_dir}/
export LOCAL_DBT_FOLDER=${airflow_dir}/../dbt/
export CREDENTIALS_PATH=${HOME}/.config/gcloud
export AIRFLOW_VERSION=2.3.3
export AIRFLOW_TAG=apache/airflow:2.3.3-python3.8
export LOCAL_TESTS=True
export LOCAL_METADATA_FOLDER=$(airflow_dir)/../metadata/

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

start_local_airflow: ## Start an airflow local container
	@cd ${airflow_dir}/airflow-local-setup && ./setup

stop_local_airflow: ## Stop the current airflow instance
	@kind delete cluster --name airflow-cluster

connect_to_airflow:  ## Allow you to connect to the airflow interface
	@kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow > /dev/null &

pause_and_unpause_dags:  ## Test pausing and unpausing DAGS
	@python -m pipelines.shared.pause_and_unpause_dags
