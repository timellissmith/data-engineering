REGION ?= europe-west2
CWD = $(shell pwd)
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
DATASET=serverless_spark_demo
TABLE=stock_prices
pyspark_dir := $$(echo ${mkfile_path} | sed s/Makefile//g)
CODE_BUCKET ?= ${GCP_PROJECT}-code
TEMP_BUCKET ?= ${GCP_PROJECT}-temp
DATA_BUCKET ?= ${GCP_PROJECT}-source-bucket
APP_NAME ?= load_data_into_bq
SRC_WITH_DEPS ?= src_with_deps

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := pyspark_help

pyspark_clean: ## CleanUp Prior to Pyspark Build
	@rm -Rf ${pyspark_dir}/dist
	@rm -Rf ${pyspark_dir}/${SRC_WITH_DEPS}

pyspark_build: pyspark_clean ## Build Python Package with Dependencies
	@echo "Packaging Code and Dependencies for ${APP_NAME}"
	@mkdir -p $(pyspark_dir)/dist
	@cd ${pyspark_dir} && pip install -r ${pyspark_dir}/requirements.txt -t ${SRC_WITH_DEPS}
	@cd ${pyspark_dir} && cp -r src ${SRC_WITH_DEPS} && rm ${SRC_WITH_DEPS}/src/main.py
	@cd $(pyspark_dir)/${SRC_WITH_DEPS}
	@find ${pyspark_dir} -name "*.pyc" -delete
	@cd $(pyspark_dir)/${SRC_WITH_DEPS} && zip -x "*.git*" -x "*.DS_Store" -x "*.pyc" -x "*/*__pycache__*/" -x ".idea*" -r ../dist/${SRC_WITH_DEPS}.zip .
	@rm -Rf ${pyspark_dir}/${SRC_WITH_DEPS}
	@cp $(pyspark_dir)/src/main.py ${pyspark_dir}/dist
	@mv $(pyspark_dir)/dist/${SRC_WITH_DEPS}.zip ${pyspark_dir}/dist/${APP_NAME}.zip
	@gsutil cp -r $(pyspark_dir)/dist gs://${CODE_BUCKET}


pyspark_run: ## Run the dataproc serverless job
	gsutil cp ${pyspark_dir}/stocks.csv gs://${DATA_BUCKET}/${DATASET}/${TABLE}/stocks.csv
	gcloud beta dataproc batches submit --project ${GCP_PROJECT} --region ${REGION} pyspark \
	gs://${CODE_BUCKET}/dist/main.py --py-files=gs://${CODE_BUCKET}/dist/${APP_NAME}.zip \
	--subnet default --properties spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4,spark.app.name=spark_serverless_repo_exemplar \
	--jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
	-- --files=${DATASET}/${TABLE}/ \
	--dataset_name=${DATASET} \
	--table_name=${TABLE} \
	--data_bucket=${DATA_BUCKET} \
	--temp_bq_bucket=${TEMP_BUCKET} \
	--delimiter=, \
	--schema "symbol string, date string, price decimal, broken string" \
	--app_name="test"

pyspark_help: ## Pyspark specific help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
