# Итоговый проект

### Описание
Репозиторий содержит итоговый проект курса "Инженер данных" Яндекс практикума.
Требуется построить ETL-процесс, получающий данные о транзакциях в различной валюте, и сохраняющий данные инкрементально
за каждый день с помощью DAG в Airflow. Итоговая витрина показывает динамику оборота всей компании.

### Структура репозитория
Внутри `src` расположены папки:
- `/src/dags` - с DAG файлами `1_data_import.py` и `2_datamart_update.py`.
- `/src/py` - с папка с модулем instrumentals для выполнения проверки схемы
- `/src/sql` - с SQL запросами для формирования `STAGING`- и `DWH`-слоев и запрос для визуализации в Metabase

### Шаги/ход исследования
- подготовлен ДАГ, загружающий из S3 в стейджинг слой Vertica файл с данными о курсе валют и о транзакциях в валютах.
  Обработка выполняется без сохранения файлов в операционной системе, а через преобразование в dataframe и загрузку 
  данных командой COPY FROM STDIN
- Подготовлен ДАГ, формирующий витрину с бизнес метриками : дата расчёта, код валюты транзакции, 
  общая сумма транзакций по валюте в долларах, общий объём транзакций по валюте, средний объём транзакций с аккаунта,
  количество уникальных аккаунтов с совершёнными транзакциями по валюте.
  Витрина заполняется инкремендально, данными за день предшествующий дню запуска ДАГа (execution_date) 

### Инструменты
Python, Pandas, Postgresql, Vertica, Airflow

### Выводы
Подготовленные ДАГи выполняют бизнес задачу - формируют витрину динамики оборота компании
