from datetime import timedelta
from decimal import Decimal
from typing import NamedTuple

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator


COINCAP_API_URL = 'http://api.coincap.io/v2/rates/bitcoin'
COINCAP_API_KEY = Variable.get('COINCAP_API_KEY', None)

CONNECTION_ID = 'analytics_db'
ANALYTICS_DB_DSN = BaseHook.get_connection(CONNECTION_ID).get_uri()


class CurrencyRate(NamedTuple):
    id: str
    symbol: str
    currencySymbol: str
    type: str
    rateUsd: Decimal
    timestamp: int


dag = DAG(
    dag_id='get_bitcoin_rate_v1',
    description="getting bitcoin quotes",
    schedule_interval='*/30 * * * *',
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'retries': 4,
        'retry_delay': timedelta(seconds=30)
    }
)


def _get_btc_rate(**context):
    response = requests.get(
        url=COINCAP_API_URL,
        headers={'Authorization': f'Bearer {COINCAP_API_KEY}'} if COINCAP_API_KEY else {},
    )
    response.raise_for_status()
    context["task_instance"].xcom_push(key="response", value=response.json())


get_btc_rate = PythonOperator(
    task_id="get_btc_rate",
    python_callable=_get_btc_rate,
    dag=dag,
    provide_context=True
)


def _trasnforming_data(**context):
    response = context["task_instance"].xcom_pull(
        task_ids="get_btc_rate", key="response"
    )

    cur_rate = CurrencyRate(**response['data'], timestamp=response['timestamp'])

    context["task_instance"].xcom_push(key="cur_rate", value=cur_rate)


trasnforming_data = PythonOperator(
    task_id="trasnforming_data",
    python_callable=_trasnforming_data,
    dag=dag,
    provide_context=True
)


def _store_to_database(**context):
    cur_rate = context["task_instance"].xcom_pull(
        task_ids="trasnforming_data", key="cur_rate"
    )
    with psycopg2.connect(ANALYTICS_DB_DSN) as connection:
            cursor = connection.cursor()

            cursor.execute(
                'INSERT INTO rates (id, symbol, "currencySymbol", type, "rateUsd", timestamp) '
                'VALUES (%s, %s, %s, %s, %s, %s)', cur_rate,
            )


store_to_database = PythonOperator(
    task_id="store_to_database",
    python_callable=_store_to_database,
    dag=dag,
    provide_context=True
)

create_table_if_not_exist = PostgresOperator(
    task_id='create_table_if_not_exist',
    postgres_conn_id=CONNECTION_ID,
    sql='''
            CREATE TABLE IF NOT EXISTS rates
            (
                    id               varchar,
                    symbol           varchar,
                    "currencySymbol" varchar,
                    type             varchar,
                    "rateUsd"        decimal,
                    timestamp        bigint
             );
        ''', dag=dag
)


create_table_if_not_exist >> get_btc_rate >> trasnforming_data >> store_to_database