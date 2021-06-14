from configparser import ConfigParser
from sqlalchemy import create_engine
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from os import listdir, remove
from os.path import isfile, join
import logging
from datetime import datetime, timezone, timedelta

default_args = {"owner": "airflow",
                'start_date': (datetime.now(tz=timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}

config = ConfigParser()
config.read("airflow/creds.cfg")


def get_engine():
    username = config.get("POSTGRES", "USERNAME")
    password = config.get("POSTGRES", "PASSWORD")
    host = config.get("POSTGRES", "HOST")
    dbname = config.get("POSTGRES", "DATABASE")
    try:
        sql_engine = create_engine("postgresql://{}:{}@{}:5432/{}".format(username, password, host, dbname))
    except Exception as error:
        print(error)
        raise Exception

    return sql_engine


def get_last_dag_run(dag):
    last_dag_run = dag.get_last_dagrun()
    if last_dag_run is None:
        last_run_time = default_args["start_date"]
    else:
        last_run_time = last_dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Last dag run: {last_run_time}")
    return last_run_time


def execute(last_run, current_run):
    engine = get_engine()
    dbconnect = engine.raw_connection()

    cursor = dbconnect.cursor()

    sql_query = """select CAST(DATE(created_at) AS VARCHAR) 
                     from airflow.order 
                    where updated_at < '{to_time}'::timestamp
                      and updated_at >= '{from_time}'::timestamp
                    group by created_at;""".format(from_time=last_run,
                                                   to_time=current_run)

    logging.info(f"Execute {sql_query}")
    cursor.execute(sql_query)
    dates = [item[0].split("\s")[0] for item in cursor.fetchall()]

    cursor.close()

    data_location = config["DATA"]["DATA_LOCATION"]

    for dt in dates:
        sql_query = f"""select *
                          from airflow.order 
                         where created_at BETWEEN '{dt} 00:00:00'::timestamp
                           and '{dt} 00:00:00'::timestamp + (interval '1 day');"""

        logging.info(f"Execute {sql_query}")
        df = pd.read_sql_query(sql_query, con=dbconnect)

        file_path = f"{data_location}/{dt}.csv"

        logging.info(f"Write {file_path}")
        df.to_csv(file_path)

    dbconnect.close()


def transform_and_load():
    data_location = config["DATA"]["DATA_LOCATION"]
    dates = [filename.split(".")[0] for filename in listdir(data_location)
             if isfile(join(data_location, filename)) and ".csv" in filename]

    engine = get_engine()

    for date in dates:
        file_path = f"{data_location}/{date}.csv"

        logging.info(f"Read {file_path}")
        df = pd.read_csv(file_path)

        df["row_hash"] = df.apply(lambda x: hash(tuple(x)), axis=1)
        df["order_id"] = df["id"]

        df = df.drop(['Unnamed: 0', 'id'], axis=1)

        logging.info(f"Send {date} to database")
        df.to_sql(config["DATA"]["STAGE_TABLE"],
                  schema=config["DATA"]["SCHEMA"],
                  if_exists="append",
                  con=engine,
                  index=False)

        logging.info(f"Remove {file_path}")
        remove(file_path)


with DAG(
        dag_id="simple_airflow_etl",
        schedule_interval="*/15 * * * *",
        default_args=default_args,
        user_defined_macros={
            'last_dag_run_execution_date': get_last_dag_run
        }
) as dag:
    executeToLocal = PythonOperator(
        task_id="execute_data_to_local_fs",
        python_callable=execute,
        op_kwargs={
            'last_run': get_last_dag_run(dag),
            'current_run': "{{ ts }}"
        }
    )

    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load
    )

    move_data = PostgresOperator(
        task_id="move_data",
        postgres_conn_id="postgres_default",
        sql="sql/move_data.sql",
        params={
            'stage_table': config["DATA"]["STAGE_TABLE"],
            'target_table': config["DATA"]["TARGET_TABLE"],
            'schema': config["DATA"]["SCHEMA"]
        }
    )

    executeToLocal >> transform_and_load >> move_data
