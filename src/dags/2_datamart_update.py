from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime
from vertica_python import connect
from airflow.hooks.dbapi_hook import DbApiHook
import os
from ..py.instrumentals import check


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


def get_execution_date(**context) -> str:
    "Функция извлекает execution_date из переменной окружения"
    execution_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"]
    log.info(f'Текущая execution_date: {execution_date}')
    return str(execution_date).split('T')[0]


def update_dwh(schema: str, table_name: str, vertica_connection: object):
    chosen_day = get_execution_date()
    log.info(msg=f'Начало загрузки данных в Vertica: {datetime.now().strftime("%Y-%m-%d %H:%M:%s")}')
    with vertica_connection:
        cur = vertica_connection.cursor()
        sql = f"""
                    INSERT INTO IAROSLAVRUSSUYANDEXRU__DWH.global_metrics
                        (date_update, currency_from, amount_total, cnt_transactions, 
                        avg_transactions_per_account, cnt_accounts_make_transactions)
                    WITH cte AS ( -- код валюты, отношение валюты к доллару(430) и диапазон актуальности данных
                         SELECT  currency_code,
                              currency_code_with,
                              currency_with_div as div_to_usd,
                              date_update as effective_from,
                              COALESCE(LAG(date_update, 1) 
                                OVER (PARTITION BY currency_code ORDER BY date_update), '9999-12-31') as effective_to
                         FROM IAROSLAVRUSSUYANDEXRU__STAGING.currencies
                         WHERE currency_code_with = 430
                         ORDER BY currency_code),
                    table_accounts_transactions AS ( -- выборка кодов валюты и уникальных 
                         SELECT 
                              currency_code,
                              transaction_dt::date as transaction_day, 
                              COUNT(distinct account_number_from) as cnt_accounts_make_transactions
                         FROM IAROSLAVRUSSUYANDEXRU__STAGING.transactions
                         GROUP BY currency_code, transaction_dt::date
                    )    
                    SELECT 
                         '{chosen_day}'::DATE as date_update, --DAG.get_run_dates.start_date -- дата расчёта
                         transactions.currency_code as currency_from, --код валюты транзакции
                         SUM(amount*div_to_usd) as amount_total, --общая сумма транзакций по валюте в долларах
                         COUNT(transactions.currency_code) as cnt_transactions, --общий объём транзакций по валюте
                         SUM(amount*div_to_usd)/max(cnt_accounts_make_transactions) as avg_transactions_per_account, 
                         --средний объём транзакций с аккаунта
                         max(cnt_accounts_make_transactions) as cnt_accounts_make_transactions 
                         -- количество уникальных аккаунтов с совершёнными транзакциями по валюте
                    FROM
                         IAROSLAVRUSSUYANDEXRU__STAGING.transactions
                    INNER JOIN cte
                    ON transactions.currency_code = cte.currency_code
                         AND transaction_dt BETWEEN cte.effective_from and effective_to 
                         -- Объединяем по коду валюты и дате проведения транзакции и курса валюты
                    INNER JOIN table_accounts_transactions
                    ON transactions.currency_code = table_accounts_transactions.currency_code 
                        and transactions.transaction_dt::date = table_accounts_transactions.transaction_day
                    WHERE 1=1
                    AND account_number_from != '-1' -- убираем тестовые аккаутны
                    AND account_number_to != '-1' -- убираем тестовые аккаутны
                    AND transaction_dt::date = '{chosen_day}'::DATE - INTERVAL '1 DAY' -- транзакции за предыдущий день
                    GROUP BY transaction_dt::date, transactions.currency_code;"""
        cur.execute(sql)
        log.info(
            f"В таблицу {schema}.{table_name}: внесено {cur.fetchall()[0][0]} записей. "
            f"Время завершения загрузки: {datetime.now().strftime('%Y-%m-%d %H:%M:%s')}")


dag = DAG(
    schedule='12 7 * * *',
    dag_id='download_dwh_ll1',
    start_date=datetime(2022, 10, 2),
    end_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['dwh', 'vertica']
)

check_dwh = PythonOperator(task_id='check_dwh',
                           python_callable=check,
                           op_kwargs={'schema': 'IAROSLAVRUSSUYANDEXRU__DWH',
                                      'vertica_connection': VerticaHook().get_conn()},
                           dag=dag)
update_dwh = PythonOperator(task_id='update_dwh',
                            python_callable=update_dwh,
                            op_kwargs={'schema': 'IAROSLAVRUSSUYANDEXRU__DWH', 'table_name': "global_metrics",
                                       'vertica_connection': VerticaHook().get_conn()},
                            dag=dag)

check_dwh >> update_dwh
