from datetime import datetime
import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def task_university_e(ti): # Забираем список словарей
    response = requests.get('http://universities.hipolabs.com/search')
    response = response.json()
    pd_data = pd.DataFrame(response)

    # Удаляем ненужные столбцы
    pd_data = pd_data.drop(columns=['domains', 'web_pages'])

    # Добавляем столбец uni_type
    pd_data['uni_type'] = pd_data['name']
    for index, row in pd_data.iterrows():
        if 'College' in row['name']:
            pd_data.at[index, 'uni_type'] = 'College'
        elif 'Univer' in row['name']:
            pd_data.at[index, 'uni_type'] = 'University'
        elif 'Institute' in row['name']:
            pd_data.at[index, 'uni_type'] = 'Institute'
        else:
            pd_data.at[index, 'uni_type'] = ''
    
    # Переименовываем колонку
    pd_data = pd_data.rename(columns={'state-province': 'state_province'})

    # Передаем данные в следующий таск уже как словарь в виде столбец-значение
    # иначе возникают ошибки индексов или сериализации
    ti.xcom_push(key='university_data', value=pd_data.to_dict(orient='records'))

def insert_data_into_db(**kwargs): # Создаем подключение к локальной БД PgSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    # Забираем из XCom pd_data
    data = kwargs['ti'].xcom_pull(key='university_data', task_ids='uni_data_transform')
    
    # Указываем в какие столбцы нужно вставить данные, значения указаны в виде заглушек
    # которые примут значения из переменной row[]
    if data:
        for row in data: 
            cursor.execute("""
                INSERT INTO university_domain (alpha_two_code, country, name, state_province, uni_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name, country) DO NOTHING;
            """, (row['alpha_two_code'], row['country'], row['name'], row['state_province'], row['uni_type']))
    # Изменения фиксируются, подключение закрывается
    connection.commit()
    cursor.close()
    connection.close()

# Инициализация ДАГа
with DAG(
    dag_id='university_test_task',
    description='Universities domain ETL',
    schedule_interval= '0 0 * * *',
    start_date=datetime.combine(datetime.today(), datetime.min.time()),
    catchup=False,
) as dag: # Создается таблица в PostgreSQL
    task_create_pgsql_table = PostgresOperator(
        task_id='create_pgsql_table',
        sql="""
            CREATE TABLE IF NOT EXISTS university_domain (
            uni_id SERIAL PRIMARY KEY,
            alpha_two_code VARCHAR(2) NULL,
            country VARCHAR(100) NULL,
            name VARCHAR(200) NULL,
            state_province VARCHAR(100) NULL,
            uni_type VARCHAR(50) NULL,
            UNIQUE (name, country));
        """,
        postgres_conn_id='postgres'
    )
    # Таска по преобразованию данных
    transforming_uni_data = PythonOperator(
        task_id='uni_data_transform',
        python_callable=task_university_e,
        provide_context=True
    )
    # Таска по загрузке данных в базу
    loading_uni_data = PythonOperator(
        task_id='load_uni_data_to_db',
        python_callable=insert_data_into_db,
        provide_context=True
    )

    task_create_pgsql_table >> transforming_uni_data >> loading_uni_data