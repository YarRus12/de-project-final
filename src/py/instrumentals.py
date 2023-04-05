import logging
log = logging.getLogger(__name__)


def check(schema: str, vertica_connection: object) -> None:
    """Функция принимает в себя наименование cхемы
    и выполняет sql запрос на создание таблиц с условием if mot exists"""
    with vertica_connection as conn:
        cur = conn.cursor()
        script_name = f'/src/sql/{schema}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Проверка схемы {schema} завершена успешно')