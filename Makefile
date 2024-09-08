# Makefile for DE Repo

VENV_NAME?=.venv
FEATURES=tests/bdd/features/
export CURRENTDIR =  $(shell pwd)
export CURRENT_UID := $(shell id -u)
export GCP_PROJECT := sample-data-engineering
export LOCAL=Please # can be any value provided it exists # TODO: Currently bools cause problems, find a way around this.
export LOCATION := europe-west2

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))
.DEFAULT_GOAL := help

-include pipelines/Makefile

help:  ## Display the help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

unit_test: ## To run all pytests.
	pytest -s -v --ignore pipelines/composer-local-dev

dbt_create_models:  ## Create DBT models from metadata
	PYTHONPATH=${CURRENTDIR} ./scripts/write_model_to_dbt ${GCP_PROJECT} --airflow_dags_dir=metadata

create_new_branch: ## Create a new branch for hotfix, bugfixes or features
	@scripts/create_new_branch.sh

run_precommit_checks:  ## Run pre-commit checks
	@pre-commit run --all-files

enable_private_subnet_network_access:  ## Enable private network access
	@gcloud compute networks subnets update default --region=${LOCATION} --enable-private-ip-google-access

copy_sample_files_into_place:  ## Copy any sample files into place
	@gsutil cp testdata/census.csv gs://cookie-cutter-testing-census-public-source/census_v1/census.csv

spin_everything_up: run_terraform enable_private_subnet_network_access dbt_create_models start_local_airflow copy_sample_files_into_place  ## Set up the project to with all the services
