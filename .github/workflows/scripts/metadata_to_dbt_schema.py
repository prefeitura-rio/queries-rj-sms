"""
Automates metadata ingestion from metadata.json to the models.
"""

import json
from pathlib import Path

import ruamel.yaml as ryaml

METADATA_FILE_PATH = "metadata.json"


def dump_metadata_into_schema_yaml(
    dataset_id: str, table_id: str, metadata: dict
) -> None:
    """
    Dumps the metadata into the schema.yaml file.
    """
    schema_yaml_path = f"models/{dataset_id}/schema.yml"

    schema = (
        load_yaml_file(schema_yaml_path)
        if Path(schema_yaml_path).exists()
        else {"version": 2, "models": []}
    )
    if schema is None:
        schema = {"version": 2, "models": []}

    if len(schema["models"]) > 0:
        # If we find a model with the same name, we delete it.
        schema["models"] = [m for m in schema["models"] if m["name"] != table_id]
    schema["models"].append(metadata)

    ruamel = load_ruamel()
    ruamel.dump(
        schema,
        open(Path(schema_yaml_path), "w", encoding="utf-8"),
    )


def format_table_description(table_info: dict) -> str:
    """
    Formats the table description.
    Input format:
    {
        "title": "some-title-here",
        "short_description": "some short description here",
        "long_description": "some long description here",
        "update_frequency": "some update frequency here",
        "temporal_coverage": "some temporal coverage here",
        "data_owner": "some data owner here",
        "publisher_name": "some publisher name here",
        "publisher_email": "some publisher email here",
        "tags": [
            "some tag here",
            "some other tag here"
        ],
        "columns": [
            {
                "name": "some column name here",
                "description": "some column description here",
            },
            ...
        ]
    }
    Output format:
    **Descrição**: some long description here
    **Frequência de atualização**: some update frequency here
    **Cobertura temporal**: some temporal coverage here
    **Órgão gestor dos dados**: some data owner here
    **Publicado por**: some publisher name here
    **Publicado por (email)**: some publisher email here
    """
    table_description = ""
    if "long_description" in table_info:
        table_description += f"**Descrição**: {table_info['long_description']}\n"
    if "update_frequency" in table_info:
        table_description += (
            f"**Frequência de atualização**: {table_info['update_frequency']}\n"
        )
    if "temporal_coverage" in table_info:
        table_description += (
            f"**Cobertura temporal**: {table_info['temporal_coverage']}\n"
        )
    if "data_owner" in table_info:
        table_description += f"**Órgão gestor dos dados**: {table_info['data_owner']}\n"
    if "publisher_name" in table_info:
        table_description += f"**Publicado por**: {table_info['publisher_name']}\n"
    if "publisher_email" in table_info:
        table_description += (
            f"**Publicado por (email)**: {table_info['publisher_email']}\n"
        )
    return table_description


def load_ruamel():
    """
    Loads a YAML file.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def load_metadata_file(filepath: str) -> dict:
    """
    Loads the file that contains path to the models' metadata.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_yaml_file(filepath: str) -> dict:
    """
    Loads the file that contains path to the models' metadata.
    """
    ruamel = load_ruamel()
    return ruamel.load((Path(filepath)).open(encoding="utf-8"))


if __name__ == "__main__":
    # Load the metadata file
    metadata: dict = load_metadata_file(METADATA_FILE_PATH)

    # Iterate over datasets
    for dataset_id in metadata:

        print(f"Ingesting metadata for dataset {dataset_id}")

        # Iterate over tables
        for table_id in metadata[dataset_id]:

            if metadata[dataset_id][table_id].get("columns") is not None:
                print(f"  - Table {table_id}")

                table_metadata = {
                    "name": table_id,
                    "description": format_table_description(
                        metadata[dataset_id][table_id]
                    ),
                    "columns": metadata[dataset_id][table_id]["columns"],
                }
                dump_metadata_into_schema_yaml(
                    dataset_id, table_id, table_metadata)
            else:
                print(f"No columns for {table_id} in dataset {dataset_id}")
