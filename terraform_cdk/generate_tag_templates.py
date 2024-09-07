from base64 import b64encode
from os import getenv
from typing import List, TypeVar

from attr import define, field, validators, Factory
from cattr import structure
from cdktf_cdktf_provider_google.data_catalog_tag import DataCatalogTagFields
from cdktf_cdktf_provider_google.data_catalog_tag_template import DataCatalogTagTemplateFields, DataCatalogTagTemplateFieldsType, \
    DataCatalogTagTemplateFieldsTypeEnumType
from pipelines.dags.main_ingestion_operator.main_schema_definitions import MainIngestionDag
from pipelines.shared.dag_loaders import parse_files, generate_dags

project = getenv("GCP_PROJECT")

VALID_FIELD_TYPES = ["STRING", "BOOLEAN", "DOUBLE", "ENUMERATED", "DATETIME", "RICHTEXT"]
TAG_LEVEL = ("TABLE", "COLUMN")

directory = f"{getenv('CURRENTDIR')}/metadata"
dags = generate_dags(full_path="/Users/timellis-smith/workspace/cookiecutter-data-engineering/cts_data_engineering/metadata/main_dags/")

TagDetails = TypeVar("TagDetails")

@define
class TagTemplateField:
    """Tag Template Field Entries"""
    field_name: str
    field_type: str = field(validator=[validators.in_(VALID_FIELD_TYPES)])
    display_name: str
    description: str
    enum_values: List[str] = field(default=Factory(list))


@define
class TagTemplate:
    template_display_name: str
    template_id: str
    tag_level: str = field(validator=[validators.in_(TAG_LEVEL)])
    template_fields: List[TagTemplateField]
    location: str = field(default="europe-west2")


def generate_tag_templates(full_path: str = "", directory: str = "") -> List[TagTemplate]:
    """Generate the DAG config by finding files and loading the config into classes."""
    tag_template_list: List[TagTemplate] = []
    for item in parse_files(full_path, directory):
        print(f"{item}")
        tag_template_list.append(structure(item, TagTemplate))
    return tag_template_list


def generate_tag_classes(t: TagTemplate, d: MainIngestionDag) -> TagDetails:
    resource_name = (
        f"projects/{project}/datasets/{d.dataset}"
        f"/tables/{d.series_name}"
    )
    print(f"{resource_name=}")
    table_id = b64encode(resource_name.encode()).decode().replace("=", "")
    parent = f"projects/{project}/locations/europe-west2/entryGroups/@bigquery/entries/{table_id}"
    tag_items = {
        "tag_items": [generate_tag_field(tf, d) for tf in t.template_fields],
        "tag_level": t.tag_level,
        "template_id": f"projects/{project}/locations/europe-west2/tagTemplates/{t.template_id}",
        "id": f"{d.dataset}_{d.series_name}_{t.template_id}",
        "parent": parent}
    return type(
        "TagDetails",
        (object,),
        tag_items,
    )


def generate_tag_field(template_field: TagDetails, pipeline: MainIngestionDag) -> DataCatalogTagFields:
    name_param = {
        "field_name": template_field.field_name,
    }
    if template_field.field_type == "ENUMERATED":
        value_param = {}
    else:
        param_map = {
            "STRING": {"string_value": getattr(pipeline, template_field.field_name)},
            "BOOLEAN": {"bool_value": getattr(pipeline, template_field.field_name)},
            "DOUBLE": {"double_value": getattr(pipeline, template_field.field_name)},
            "TIMESTAMP": {"timestamp_value": getattr(pipeline, template_field.field_name)},
        }
        value_param = param_map[template_field.field_type]
    params = name_param | value_param
    return DataCatalogTagFields(**params)
