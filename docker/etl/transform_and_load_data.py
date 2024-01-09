"""
Transforms the provided dataset into the star schema and loads into the
database.

It is intended to use with Apache Airflow and Docker. You need to provide the
DATAFILES and DIM_COLUMNS environment variable as string with obects separated
by space. Script will fail if you won't.
"""

import pandas as pd
from os import environ, getenv
from pathlib import Path
from sqlalchemy import create_engine

REQUIRED_VARIABLES = ["DATAFILES", "DIM_COLUMNS", "CONNECTION_STRING"]


def prepare_variables() -> list[str]:
    """
    The transform process needs REQUIRED_VARIABLES to be present in
    the environment. Let's check, if those are present and fail if any is
    missing. Return them as a lists if they are found.

    Arguments:
        None

    Returns:
        datafiles: list[str] - paths to the datafiles with respective
            database table names, that data will be stored
        dim_columns: list[str] - columns, that will be transformed into
            dimension tables
    """
    for env_var in REQUIRED_VARIABLES:
        if env_var not in environ:
            raise Exception(f"Environment variable {env_var} not defined.")

    variables = {
        env_var: getenv(env_var).split() for env_var in REQUIRED_VARIABLES
    }
    datafiles = list(variables["DATAFILES"])
    dim_columns = list(variables["DIM_COLUMNS"])
    connection_string = "".join(variables["CONNECTION_STRING"])

    return datafiles, dim_columns, connection_string


def prepare_dimension_tables(
    datafiles: list[str], dim_columns: list[str]
) -> dict[str : pd.DataFrame]:
    """
    Prepares the dimension tables for the star schema. Combines all input
    datafiles to get a complete input table, then creates a dim tables as
    dataframes, ready to be loaded into the SQL database.

    Arguments:
        datafiles: list[str] - list of file locations with datafiles
        dim_columns: list[str] - a list of columns, that will be treated as
            dimension tables

    Returns:
        dim_tables: dict[str] - a dictionary with column names and
        coresponding dataframes with unique indexed values
    """
    combined_fact_table = pd.concat(
        [pd.read_csv(datafile) for datafile in datafiles], ignore_index=True
    )

    dim_tables = {
        table: pd.Series(
            pd.unique(combined_fact_table[table]), name=table.lower()
        ).dropna()
        for table in dim_columns
    }

    return dim_tables


def prepare_fact_tables(
    datafiles: list[str], dim_tables: dict[str, pd.DataFrame]
):
    """
    Prepares fact tables. It uses dimension tables as a reference, and
    replaces values from DIM_COLUMNS in fact tables with indices from
    dimension tables. Indices will act as primary keys, so the relationship
    could be estabilished.

    Arguments:
        datafiles: list[str] - data files paths
        dim_tables: dict[str, Dataframe] - a dictionary with column names and
            coresponding dataframes

    Returns:
        tables: dict[str, dataframe] - a dictionary with fact table names and
            corresponding dataframe
    """

    tables = {}
    for datafile in datafiles:
        table_name = str(Path(datafile).stem).split("-")[1]

        fact_table = pd.read_csv(datafile)

        for column in dim_tables.keys():
            fact_table[column].replace(
                to_replace=dim_tables[column].values,
                value=dim_tables[column].index,
                inplace=True,
            )

        tables[table_name] = fact_table

    return tables


def load_data(data_dict: dict[str, pd.DataFrame], connection_engine: str):
    """
    Loads data into the database.

    Arguments:
        data_dict: dict[str, pd.DataFrame] - a dictionary with table names and
            corresponding dataframe
        connection_engine: str - an SQLAlchemy connection engine
    """
    for table in data_dict.keys():
        data_dict[table].to_sql(
            table.lower(),
            connection_engine,
            index_label="id",
            if_exists="append",
        )


if __name__ == "__main__":
    datafiles, dim_columns, connection_string = prepare_variables()
    dim_tables = prepare_dimension_tables(datafiles, dim_columns)
    fact_tables = prepare_fact_tables(datafiles, dim_tables)

    connection_engine = create_engine(connection_string)

    load_data(dim_tables, connection_engine)
    load_data(fact_tables, connection_engine)
