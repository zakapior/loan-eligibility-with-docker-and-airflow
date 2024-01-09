"""
This DAG creates a PostgreSQL database server, configures the database,
downloads the dataset from Kaggle, transforms it into the star schema and
uploads it into the database.
"""

from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="jakluz-de3.1.5-etl",
    start_date=datetime(2023, 11, 24),
    schedule_interval=None,
    catchup=False,
    tags=["jakluz-de3.1.5"],
) as dag:
    """
    This task creates a PostgreSQL database container. It uses a script
    file, that utilizes Airflow Connection jakluz-de3.1.5-postgres data to
    configure the service.
    """
    create_postgres = BashOperator(
        task_id="create_postgres",
        bash_command="scripts/create_database.sh",
        cwd="/opt/airflow/dags",
    )

    """
    Creates new PostgreSQL role for the project purposes.
    """
    create_postgres_user = PostgresOperator(
        task_id="create_postgres_user",
        postgres_conn_id="jakluz-de3.1.5-postgres",
        sql="CREATE USER {{ conn.get('jakluz-de3.1.5-postgres-turing').login }} WITH PASSWORD '{{ conn.get('jakluz-de3.1.5-postgres-turing').password }}';",
    )

    """
    Creates new PostgreSQL database.
    """
    create_postgres_database = PostgresOperator(
        task_id="create_postgres_database",
        postgres_conn_id="jakluz-de3.1.5-postgres",
        sql="CREATE DATABASE {{ conn.get('jakluz-de3.1.5-postgres-turing').schema }} WITH OWNER {{ conn.get('jakluz-de3.1.5-postgres-turing').login }};",
        autocommit=True,
    )

    """
    Prepares database schema.
    """
    prepare_schema = PostgresOperator(
        task_id="prepare_schema",
        postgres_conn_id="jakluz-de3.1.5-postgres-turing",
        sql="sql/create_db_schema.sql",
        autocommit=True,
    )

    """
    This task downloads the dataset from Kaggle with a Python script run
    inside Docker Python container.
    """
    download_data = DockerOperator(
        task_id="download_data",
        image="jkluz/jakluz-de3.1.5",
        container_name="jakluz-de3.1.5-download_data",
        environment={
            "KAGGLE_USERNAME": "{{ conn.get('jakluz-de3.1.5-kaggle').login }}",
            "KAGGLE_KEY": "{{ conn.get('jakluz-de3.1.5-kaggle').password }}",
        },
        auto_remove="force",
        mounts=[
            Mount(
                "/usr/src/app/output",
                "/home/ubuntu/jakluz-DE3.1.5/data",
                "bind",
            )
        ],
        command="python3 download_data.py",
    )

    """
    This task tranforms the dataset into the start scheme and loads it into
    the PosgreSQL database with a Python script run inside Docker Python
    container.
    """
    transform_and_load_data = DockerOperator(
        task_id="transform_and_load_data",
        image="jkluz/jakluz-de3.1.5",
        container_name="jakluz-de3.1.5-etl",
        network_mode="airflow_default",
        environment={
            "DATAFILES": "{{ ti.xcom_pull(key='return_value', task_ids='download_data') }}",
            "DIM_COLUMNS": " ".join(
                [
                    "Gender",
                    "Married",
                    "Dependents",
                    "Education",
                    "Self_Employed",
                    "Credit_History",
                    "Property_Area",
                ]
            ),
            "CONNECTION_STRING": (
                "postgresql://"
                "{{ conn.get('jakluz-de3.1.5-postgres-turing').login }}"
                ":{{ conn.get('jakluz-de3.1.5-postgres-turing').password }}"
                "@{{ conn.get('jakluz-de3.1.5-postgres-turing').host }}"
                "/{{ conn.get('jakluz-de3.1.5-postgres-turing').schema }}"
            ),
        },
        auto_remove="force",
        mounts=[
            Mount(
                "/usr/src/app/output",
                "/home/ubuntu/jakluz-DE3.1.5/data",
                "bind",
            )
        ],
        command="python3 transform_and_load_data.py",
        force_pull=True,
    )

(
    create_postgres
    >> create_postgres_user
    >> create_postgres_database
    >> prepare_schema
    >> download_data
    >> transform_and_load_data
)
