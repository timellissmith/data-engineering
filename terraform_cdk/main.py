#!/usr/bin/env python
"""Terraform CDK Main module."""

from os import getenv

from cdktf import App, LocalBackend, TerraformStack
from cdktf_cdktf_provider_google.bigquery_dataset import BigqueryDataset
from cdktf_cdktf_provider_google.project_service import ProjectService
from cdktf_cdktf_provider_google.provider import GoogleProvider
from cdktf_cdktf_provider_google.storage_bucket import StorageBucket
from cdktf_cdktf_provider_google.data_catalog_tag_template import DataCatalogTagTemplate, DataCatalogTagTemplateFields, DataCatalogTagTemplateFieldsType
from cdktf_cdktf_provider_google.data_catalog_tag import DataCatalogTag, DataCatalogTagFields
from cdktf_cdktf_provider_google.data_google_service_account_access_token import DataGoogleServiceAccountAccessToken
from constructs import Construct
from pipelines.shared.dag_loaders import generate_dags
from terraform_cdk.generate_tag_templates import generate_tag_templates, generate_tag_classes

directory = f"{getenv('CURRENTDIR')}/metadata/main_dags/"
tag_template_directory = f"{getenv('CURRENTDIR')}/metadata/tag_templates"
dags = generate_dags(directory=directory)
tag_templates = generate_tag_templates(directory=tag_template_directory)
local_environment = getenv("LOCAL", None)
project = getenv("GCP_PROJECT")
location = getenv("LOCATION", "europe-west2")

tags = []
for dag in dags:
    for tag_template in tag_templates:
        tags.extend(generate_tag_classes(tag_template, pipeline) for pipeline in dags)


class MyStack(TerraformStack):
    """Class for Creating Terraform Stack."""

    def __init__(self, scope: Construct, id: str):
        """Define the Terraform Code."""
        super().__init__(scope, id)

        # define resources here
        impersonation = GoogleProvider(self, "GCP", region=location, project=getenv("GCP_PROJECT"), alias="impersonation")
        service_account = DataGoogleServiceAccountAccessToken(self, id_="sa_access_token", provider=impersonation,
                                                              target_service_account="local-service-acount@cookie-cutter-testing.iam.gserviceaccount.com",
                                                              scopes=["userinfo-email", "cloud-platform"])
        provider = GoogleProvider(self, "GCP_SA", access_token=service_account.access_token, project=project, region="europe-west2")
        services = [
            "bigquery.googleapis.com",
            "datacatalog.googleapis.com",
            "iam.googleapis.com",
            "pubsub.googleapis.com",
            "run.googleapis.com",
            "dataproc.googleapis.com",
        ]

        # Enable the services
        [ProjectService(self, svc, service=svc, provider=provider) for svc in services]

        # Create the datasets based on the Pipelines
        list(
            {
                BigqueryDataset(
                    self,
                    dag.dataset,
                    dataset_id=dag.dataset,
                    description=dag.description,
                    location=location,
                    provider=provider
                )
                for dag in dags
            }
        )

        # Create the source buckets based on the pipelines
        list(
            {
                StorageBucket(
                    self,
                    dag.source_bucket,
                    location=location,
                    name=dag.source_bucket,
                    provider=provider
                )
                for dag in dags
            }
        )

        list(
            {
                StorageBucket(
                    self,
                    dag.unprocessed_bucket,
                    location=location,
                    name=dag.unprocessed_bucket,
                    provider=provider
                )
                for dag in dags
            }
        )

        list(
            {
                StorageBucket(
                    self,
                    dag.archive_bucket,
                    location=location,
                    name=dag.archive_bucket,
                    provider=provider
                )
                for dag in dags
            }
        )

        list(
            {
                StorageBucket(
                    self, dag.code_bucket, location="europe-west2", name=dag.code_bucket
                )
                for dag in dags
            }
        )

        list(
            {
                StorageBucket(
                    self, dag.temp_bucket, location="europe-west2", name=dag.temp_bucket
                )
                for dag in dags
            }
        )
        tt = [DataCatalogTagTemplate(self, tag_template.template_id, display_name=tag_template.template_display_name, tag_template_id=tag_template.template_id,
                                     provider=provider, project=project, force_delete=True,
                                     fields=[DataCatalogTagTemplateFields(field_id=field.field_name, description=field.description,
                                                                          type=DataCatalogTagTemplateFieldsType(primitive_type=field.field_type)) for
                                             field in tag_template.template_fields]) for tag_template in tag_templates]

        tgs = [DataCatalogTag(self, tag.id, template=tag.template_id, fields=tag.tag_items, parent=tag.parent) for tag in tags]


app = App()
stack = MyStack(app, "cts-data-engineering")
LocalBackend(stack)

app.synth()
