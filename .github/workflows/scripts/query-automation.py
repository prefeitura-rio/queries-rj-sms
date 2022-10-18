# import sys

# sys.path.append(".github/workflows/")

import pandas as pd

from metadata_automation import *


## this need to be the same as pipelines/utils/util.py (remove_columns_accents)
def remove_accents_and_lower(data: pd.Series):
    """
    Remove accents from dataframe columns.
    """
    return list(
        data.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(" ", "_")
        .str.replace(".", "")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.lower()
    )


def get_basic_treated_query(spreadsheet_id: str, dataset_id: str, table_id: str):
    """
    generates a basic treated query
    """

    gspread_client = get_gspread_client()
    spreadsheet = download_spreadsheet(spreadsheet_id, gspread_client)

    tabela = pd.read_excel(spreadsheet, sheet_name="tabela", header=None)
    table_columns = tabela[0].tolist()
    tabela = tabela[1].T.tail(1)
    tabela.columns = table_columns

    project_id = tabela["bigquery_project"].values[0]

    columns = pd.read_excel(spreadsheet, sheet_name="colunas")

    ## se Nome da coluna 'e null a coluna nao deve entrar em producao'
    columns = columns[columns["Nome da coluna"].notnull()]

    columns["Nome original da coluna"] = remove_accents_and_lower(
        columns["Nome original da coluna"]
    )

    originais = columns["Nome original da coluna"].tolist()
    nomes = columns["Nome da coluna"].tolist()
    tipos = columns["Tipo da coluna"].tolist()

    indent_space = 4 * " "
    query = "SELECT \n"
    for original, nome, tipo in zip(originais, nomes, tipos):
        if tipo == "GEOGRAPHY":
            query += f"ST_GEOGFROMTEXT({original}) AS {nome},\n"
        elif "id" in nome or tipo == "INT64":
            query += (
                indent_space
                + f"SAFE_CAST(REGEXP_REPLACE({original}, r'\.0$', '') AS {tipo}) AS {nome},\n"
            )
        elif tipo == "DATETIME":
            query += (
                indent_space
                + f"SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', {original}) AS {tipo}) AS {nome},\n"
            )
        elif tipo == "FLOAT64":
            query += indent_space + (
                f"SAFE_CAST(REGEXP_REPLACE({original}, r',', '.') AS {tipo}) AS {nome},"
            )
        else:
            query += indent_space + f"SAFE_CAST({original} AS {tipo}) AS {nome},\n"

    query += f"FROM {project_id}.{dataset_id}_staging.{table_id} AS t"

    return query


def get_model_file_path(
    model_sql_folder_path: str = None, dataset_id: str = None, table_id: str = None
):
    model_sql_path = (
        Path("models/")
        if model_sql_folder_path is None
        else Path(model_sql_folder_path)
    )
    return model_sql_path / dataset_id / f"{table_id}.sql"


def dump_query_into_model_sql(query: str, model_sql_path_file: str):

    if not query:
        print("No query to save")
    else:
        model_sql_path_file.parent.mkdir(parents=True, exist_ok=True)
        # save publish.sql in table_folder
        model_sql_path_file.open("w", encoding="utf-8").write(query)


if __name__ == "__main__":
    # Load the metadata file
    metadata: dict = load_metadata_file(METADATA_FILE_PATH)

    # List all models
    models: dict = metadata["models"]

    # Iterate over datasets
    for dataset_id, dataset in models.items():

        print(f"Ingesting metadata for dataset {dataset_id}")

        # Iterate over tables
        for table_id in dataset:

            # Get the table
            table: dict = dataset[table_id]

            # path to save the model
            model_sql_path_file = get_model_file_path(
                model_sql_folder_path=None, dataset_id=dataset_id, table_id=table_id
            )

            # Check whether there is a spreadsheet ID set for this table
            if model_sql_path_file.exists():
                print(f"{model_sql_path_file} already exists. Skipping...")
            elif "spreadsheet_id" in table and table["spreadsheet_id"]:
                print(
                    f"- Creating basic treated querie for table {table_id}...",
                    end="\n",
                )
                # Fetch a basic query from Google Sheets metadata
                query = get_basic_treated_query(
                    table["spreadsheet_id"], dataset_id, table_id
                )
                dump_query_into_model_sql(query, model_sql_path_file)
