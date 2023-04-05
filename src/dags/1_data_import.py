from airflow import DAG
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import logging
from datetime import datetime
import pandas as pd
from vertica_python import connect
from airflow.hooks.dbapi_hook import DbApiHook
from ..py.instrumentals import check


today = datetime.now().strftime("%Y-%m-%d")
log = logging.getLogger(__name__)


class VerticaHook(DbApiHook):
    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_connection'
    supports_autocommit = True

    def get_conn(self):
        conn = self.get_connection(self.vertica_conn_id)
        conn_config = {"user": conn.login, "password": conn.password or '', "database": conn.schema, "autocommit": True,
                       "host": conn.host or 'localhost'}
        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)
        conn = connect(**conn_config)
        return conn


psql_connection = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
connect_to_db = psycopg2.connect(f"host='{psql_connection.host}' port='{psql_connection.port}' "
                                 f"dbname='{psql_connection.schema}' user='{psql_connection.login}' "
                                 f"password='{psql_connection.password}'")





def check_last_date(table_name: str, column_name: str) -> str:
    """Функция принимает в себя назнание таблицы и название колонки, содержащей данные о времени записи и
        возвращает наибольшую дату и время записей уже загруженных в стейдж
        для уменьшения объема загружаемых данных"""
    vertica_connection = VerticaHook().get_conn()
    with vertica_connection as conn:
        cur = conn.cursor()
        sql = f"""SELECT coalesce(max({column_name}), '1970-01-01 00:00:00') 
                  FROM IAROSLAVRUSSUYANDEXRU__STAGING.{table_name};"""
        cur.execute(sql)
        max_datetime = cur.fetchone()[0]
        log.info(f"Дата и время последней записи в таблице IAROSLAVRUSSUYANDEXRU__STAGING.{table_name}: {max_datetime}")
        return max_datetime


def data_receiver(connect_pg: object, schema: str, table_name: str):
    """Функция принимает в себя параменты подключения к Postgres, наименование схемы и таблицы.
    Получает данные из Postgres, создает и возвращает dataframe
    """
    if table_name == 'transactions':
        column_name = 'transaction_dt'
    elif table_name == 'currencies':
        column_name = 'date_update'
    else:
        log.error(msg='Указана неизвестная таблица')
        raise Exception('Указана неизвестная таблица')
    max_date = check_last_date(table_name=table_name, column_name=column_name)
    sql = f"""
            select * from {schema}.{table_name} where {column_name} > '{max_date}';
            """
    df = pd.read_sql_query(sql, connect_pg)
    log.info(f"Подготовлен Dataframe c {df.shape[0]} записями")
    return df


def load_to_vertica(schema: str, table_name: str, vertica_conn_info: object, columns: tuple) -> None:
    """
    Функция принимает в себя наименования схемы и таблицы, параменты подключения к Vertica и название колонок
    и выполняет копирование собранного в памяти dataframe в Vertica
    """
    df = data_receiver(connect_pg=connect_to_db, schema='public', table_name=table_name)
    vertica_connection = VerticaHook().get_conn()
    # вставляем данные из DataFrame в Vertica
    log.info(msg=f'Начало загрузки данных в Vertica: {datetime.now().strftime("%Y-%m-%d %H:%M:%s")}')
    with vertica_connection:
        cur = vertica_conn_info.cursor()
        cur.copy(f"COPY {schema}.{table_name} {columns} FROM STDIN DELIMITER ',' "
                 f"REJECTED DATA AS TABLE {schema}.{table_name}_reg", df.to_csv(index=False, header=False), stdin=True)
        log.info(
            f"В таблицу {schema}.{table_name}: скопировано {df.shape[0]} записей. "
            f"Время завершения загрузки: {datetime.now().strftime('%Y-%m-%d %H:%M:%s')}")


dag = DAG(
    schedule='12 7 * * *',
    dag_id='download_stage_postgres',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stg', 'postgresql', 'vertica'],
    is_paused_upon_creation=True
)

check = PythonOperator(task_id='check_stg_database',
                       python_callable=check,
                       op_kwargs={'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING',
                                  'vertica_connection': VerticaHook().get_conn()},
                       dag=dag)
load_transactions = PythonOperator(task_id='load_transactions',
                                   python_callable=load_to_vertica,
                                   op_kwargs={'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name': 'transactions',
                                              'columns': '(operation_id,account_number_from,account_number_to,'
                                                         'currency_code,country,status,transaction_type,amount,'
                                                         'transaction_dt)',
                                              'vertica_conn_info': VerticaHook().get_conn()},
                                   dag=dag)
load_currencies = PythonOperator(task_id='load_currencies',
                                 python_callable=load_to_vertica,
                                 op_kwargs={'schema': 'IAROSLAVRUSSUYANDEXRU__STAGING', 'table_name': 'currencies',
                                            'columns': '(date_update,currency_code,currency_code_with,'
                                                       'currency_with_div)',
                                            'vertica_conn_info': VerticaHook().get_conn()},
                                 dag=dag)

check >> load_transactions >> load_currencies
