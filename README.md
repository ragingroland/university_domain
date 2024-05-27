# university.py

Служит для загрузки списка образовательных учреждений с помощью библиотеки requests и get запроса, затем обрабатывает этот список - удаляет ненужные столбцы, добавляет новый, наполяем новый столбец ожидаемыми значениями, после чего загружает этот список в БД PostgreSQL.

## Как запустить?

1) Клонировать репозиторий себе на диск
2) Установить Docker Desktop или Docker Engine
3) Запустить терминал и перейти в папку репозиторием
4) Набрать команду "**docker build -t *название билда* -f Dockerfile . --no-cache**", чтобы создать билд Airflow
5) Набрать команду "**docker-compose up**" (либо запустить через Docker Desktop) и дождаться, пока запустятся все контейнеры, затем перейти через браузер по адресу **localhost:8080**, настроить подключения
6) После работы остановить контейнеры нажатием CTRL+C в терминале, либо набрать команду **docker stop $(docker ps -q)** или остановить в Docker Desktop

## Описание

**DAG ID** - university_test_task

**Описание** - University domain ETL

**Расписание** - каждые сутки в три часа ночи по московскому времени

**Время запуска** - 27.05.2024



Всего в ДАГе три задачи: создать таблицу, если её нет в базе и, забрать данные и трансформировать их, а затем загрузить в базу данных.


Первая создает таблицу, подключаясь к локальной базе данных, в следующем виде:

```
CREATE TABLE IF NOT EXISTS university_domain (
    uni_id SERIAL PRIMARY KEY, -- автоинкрементный порядковый номер
    alpha_two_code VARCHAR(2) NULL, -- код страны из двух символов
    country VARCHAR(100) NULL, -- название страны
    name VARCHAR(200) NULL, -- наименование образовательного учреждения
    state_province VARCHAR(100) NULL, -- штат или провинция
    uni_type VARCHAR(50) NULL, -- тип образовательного учреждения
    UNIQUE (name, country)); -- уникальность для корректной работы ON CONFLICT
```

Поскольку список не дает нам никаких идентификационных номеров или дат загрузки, я использовал обычный автоинкрементный serial столбец как primary key. Все остальные столбцы в той или иной степени соответсвуют оригиналу. 


Следующей задачей я сделал запрос в базу образовательных учреждений с помощью get-запроса библиотеки requests. Преобразовал запрошенные данные в json и передал их в датафрейм pandas. Далее, были удалены ненужные столбы domains и web_pages, создан новый uni_types, который посредством прохода по всему датафрейму был наполнен ожидаемыми значениями (тип образовательного учреждения из трех указанных в задаче, в противном случае ячейка остается пустой). 

Далее, во избежании конфликтов имен, был переименован столбец state-province в state_province.
Далее, передал датафрейм в XCom Airflow, чтобы использовать его в следующей таске.


Третьим шагом в этой задаче стала загрузка полученных и преобразованных данных в PostgreSQL с помощью PostgresOperator, где с помощью цикла в случае конфликтов данные просто не загружаются.



### Источники:

https://www.astronomer.io/docs/learn/connections

https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.html#httpoperator

https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/sensors/http/index.html

https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/operators/http/index.html#airflow.providers.http.operators.http.SimpleHttpOperator

https://www.restack.io/docs/airflow-knowledge-http-operator-rest-api-example-simple

https://stackoverflow.com/questions/59574331/how-to-call-a-rest-end-point-using-airflow-dag

https://www.postgresql.org/docs/current/functions-json.html

https://dba.stackexchange.com/questions/268365/using-python-to-insert-json-into-postgresql

https://hatchjs.com/airflow-read-json-file/

https://airflow.apache.org/docs/apache-airflow/2.2.0/tutorial_taskflow_api.html

https://pandas.pydata.org/docs/user_guide/io.html#reading-json

https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html

https://stackoverflow.com/questions/30459485/replacing-row-values-in-pandas

https://www.geeksforgeeks.org/adding-new-column-to-existing-dataframe-in-pandas/

https://habr.com/ru/companies/ruvds/articles/494720/

https://stackoverflow.com/questions/41469430/writing-json-column-to-postgres-using-pandas-to-sql

https://stackoverflow.com/questions/3294889/iterating-over-dictionaries-using-for-loops

https://bigdataschool.ru/blog/airflow-xcom-variables.html

https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html

https://www.astronomer.io/docs/learn/airflow-passing-data-between-tasks

https://www.learndatasci.com/solutions/python-valueerror-truth-value-series-ambiguous-use-empty-bool-item-any-or-all/

https://www.restack.io/docs/airflow-knowledge-xcom-pull-push-example

https://github.com/trbs/airflow-examples/blob/master/dags/example_xcom.py

https://stackoverflow.com/questions/46059161/airflow-how-to-pass-xcom-variable-into-python-function

https://pythonguides.com/pandas-dataframe-iterrows/#:~:text=In%20Python%2C%20the%20itertuple()%20method,values%20of%20namedtuple%20are%20ordered
