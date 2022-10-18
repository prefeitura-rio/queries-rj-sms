"""
Fetches metadata from meta.dados.rio and generates a JSON file for this repository.
"""
from glob import glob
import json
from pathlib import Path
from typing import List, Union

import requests


def get_all_sql_files(path: Union[Path, str]) -> List[Path]:
    """
    Returns all SQL files in the given path.
    """
    return [Path(item) for item in glob(f"{path}/**/*.sql", recursive=True)]


def build_initial_dict(sql_files: List[Path]) -> dict:
    """
    Builds the initial dictionary that will be dumped into the metadata.json file.
    This dictionary will be in the format:
    {
        "dataset_id_1": {
            "table_id_1": {},
            "table_id_2": {},
            ...
        },
        "dataset_id_2": {
            "table_id_1": {},
            "table_id_2": {},
            ...
        },
        ...
    }
    """
    initial_dict = {}
    for sql_file in sql_files:
        dataset_id = sql_file.parent.name
        table_id = sql_file.stem
        if dataset_id not in initial_dict:
            initial_dict[dataset_id] = {}
        initial_dict[dataset_id][table_id] = {}
    return initial_dict


def custom_fetch(url: str) -> dict:
    """
    Fetches from DRF API and raises if count is not 1.
    """
    response = requests.get(url)
    response.raise_for_status()
    response_json = response.json()
    if response_json["count"] == 1:
        return response_json["results"][0]
    elif response_json["count"] > 1:
        raise Exception(f"There is more than one result to URL {url}.")
    else:
        raise Exception(f"There is no result to URL {url}.")


def fetch_metadata(initial_dict: dict) -> dict:
    """
    Fetches metadata from meta.dados.rio and updates the initial_dict with the new metadata.
    """
    # Iterate over datasets
    for dataset_id in initial_dict:
        # Get the title prefix for the dataset
        data = custom_fetch(f"https://meta.dados.rio/api/datasets/?name={dataset_id}")
        title_prefix = data["title_prefix"]
        # Iterate over datasets' tables
        for table_id in initial_dict[dataset_id]:
            # Fetch metadata from meta.dados.rio
            url = f"https://meta.dados.rio/api/tables/?dataset={dataset_id}&name={table_id}"
            response = requests.get(url)
            # Asserts that the response is ok
            response.raise_for_status()
            # Parses JSON response
            response_json = response.json()
            # If the table exists, continue
            if response_json["count"] == 1:
                data = response_json["results"][0]
                # Set attributes
                initial_dict[dataset_id][table_id][
                    "title"
                ] = f"{title_prefix}: {data['title']}"
                initial_dict[dataset_id][table_id]["short_description"] = data[
                    "short_description"
                ]
                initial_dict[dataset_id][table_id]["long_description"] = data[
                    "long_description"
                ]
                initial_dict[dataset_id][table_id]["update_frequency"] = data[
                    "update_frequency"
                ]
                initial_dict[dataset_id][table_id]["temporal_coverage"] = data[
                    "temporal_coverage"
                ]
                initial_dict[dataset_id][table_id]["data_owner"] = data["data_owner"]
                initial_dict[dataset_id][table_id]["publisher_name"] = data[
                    "publisher_name"
                ]
                initial_dict[dataset_id][table_id]["publisher_email"] = data[
                    "publisher_email"
                ]
                initial_dict[dataset_id][table_id]["tags"] = data["tags"]
                initial_dict[dataset_id][table_id]["categories"] = data["categories"]
                initial_dict[dataset_id][table_id]["columns"] = []
                for column in data["columns"]:
                    initial_dict[dataset_id][table_id]["columns"].append(
                        {"name": column["name"], "description": column["description"],}
                    )
            # If the table doesn't exist or there is more than one table, raise
            elif response_json["count"] > 1:
                raise Exception(
                    f"There is more than one table with the name {table_id} in the dataset {dataset_id}."
                )
            else:
                raise Exception(
                    f"There is no table with the name {table_id} in the dataset {dataset_id}."
                )

    return initial_dict


def save_metadata(metadata_dict: dict, path: Union[Path, str]) -> None:
    """
    Saves the metadata_dict to the given path.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metadata_dict, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    sql_files = get_all_sql_files("models")
    initial_dict = build_initial_dict(sql_files)
    metadata_dict = fetch_metadata(initial_dict)
    save_metadata(metadata_dict, "metadata.json")
