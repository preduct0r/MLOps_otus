import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


def _request_data(**context):
    url = 'https://api.coincap.io/v2/rates/bitcoin'
    payload = {}
    headers = {'Content-Type': 'application/json; charset=utf-8'}

    req_btc = requests.request("GET", url, headers=headers, data=payload)
    btc_json = req_btc.json()['data']
    context["task_instance"].xcom_push(key="btc_json", value=btc_json)


def _parse_data(**context):
    btc_json = context["task_instance"].xcom_pull(
        task_ids="request_data", key="btc_json")
    # ? should we really parse 'symbol'?
    btc_data = [btc_json['id'], btc_json['symbol'],
                btc_json['currencySymbol'], btc_json['type'], btc_json['rateUsd']]
    context["task_instance"].xcom_push(key="btc_data", value=btc_data)


def _insert_data(**context):
    btc_data = context["task_instance"].xcom_pull(
        task_ids="parse_data", key="btc_data")
    print(btc_data)
    dest = PostgresHook(postgres_conn_id='postgres_otus')
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(
        """INSERT INTO btc_data(id, symbol, currency_symbol, "type", rate_usd) VALUES (%s, %s, %s, %s, %s)""", (btc_data))
    dest_conn.commit()


args = {'owner': 'marina'}

dag = DAG(
    dag_id="btc_analytics",
    default_args=args,
    description='DAG btc_analytics - Task 2 Otus DE',
    start_date=datetime(2021, 7, 1),
    schedule_interval='*/30 * * * *',
    catchup=False,
)


start = DummyOperator(task_id="start", dag=dag)

request_data = PythonOperator(
    task_id='request_data', python_callable=_request_data, provide_context=True, dag=dag)
parse_data = PythonOperator(
    task_id='parse_data', python_callable=_parse_data, provide_context=True, dag=dag)
insert_data = PythonOperator(
    task_id='insert_data', python_callable=_insert_data, provide_context=True, dag=dag)

create_table_if_not_exist = PostgresOperator(
    task_id='create_table_if_not_exist',
    postgres_conn_id="postgres_otus",
    sql='''CREATE TABLE IF NOT EXISTS btc_data(
            date_load timestamp NOT NULL DEFAULT NOW()::timestamp,
            id varchar(100) NOT NULL,
            symbol varchar(3) NOT NULL,
            currency_symbol varchar(10) NULL,
            "type" varchar(10) NULL,
            rate_usd decimal NULL
        );''', dag=dag
)


start >> create_table_if_not_exist >> request_data >> parse_data >> insert_data