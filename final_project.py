from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import json
import requests
import time
from pathlib import Path
import zipfile
import logging
from collections import Counter

default_args = {
    'owner': 'akh',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

#Функция обрабатывает файлы из egrul
def get_egrul():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_akh')
    logger = logging.getLogger(__name__)
    
    path_zip = Path('/home/rtstudent/students/akhmetov/egrul.json.zip')

    # Последовательно обрабатываем все файлы в архиве
    with zipfile.ZipFile(path_zip, 'r') as zipobj:
        file_names = zipobj.namelist()
        logger.info(file_names)

        i = 0
        x = len(file_names)
        for name in file_names:
            # Распаковываем один файл
            zipobj.extract(name)
            logger.info("Файл "+str(name)+" распакован!")
            # Обрабатываем очередной файл
            with open(name, 'r', encoding='utf-8') as f:
                dictionary  = json.load(f)
                for obj in dictionary:
                    try:
                        okved = obj['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                    except KeyError:
                        continue
                    finally:
                        if okved.split('.')[0] == '61':
                            inn = obj.get('inn')
                            name = obj.get('name')
                            full_name = obj.get('full_name')
                            kpp = obj.get('kpp')
                            rows = [(inn,  name, full_name, okved, kpp),]
                            fields = ['inn', 'name', 'full_name', 'okved', 'kpp']

                            # Загружаем в базу
                            sqlite_hook.insert_rows(
                                table='telecom_companies',
                                rows=rows,
                                target_fields=fields,
                            )
            i += 1
            return 0

#Функция делает постраничные запросы к api hh, получает ответы, отфильтровывает телеком вакансии
def get_vacancy():
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_akh')
    logger = logging.getLogger(__name__)

    telecom_vac = []
    url = 'https://api.hh.ru/vacancies'
    user_agent = {'User-agent': 'Mozilla/5.0'}

    # Последовательно загружаем страницы
    for n in range(1, 10):
        logger.info("Обрабатываем страницу "+str(n)+" !")

        # Определяем входные параметы запроса
        params = {"page": n, "per_page": 10, "search_field":"name" ,"text": "middle python developer" }
        result = requests.get(url, headers=user_agent, params=params)
        vacancies  = json.loads(result.text)
        # Получаем все вакансии на странице
        vacancies = vacancies["items"]

        for vac in vacancies:
            vac_url = "https://api.hh.ru/vacancies/"+str(vac["id"])
            res =  requests.get(vac_url, headers=user_agent)
            # logger.info("Запрос вакансий завершился с кодом: "+str(res.status_code)+"!")


            if (res.status_code == 200):
                vacancy  = json.loads(res.text)
                reqq = requests.get(vacancy["employer"]["url"])
                # logger.info("Запрос работодателя завершился с кодом: "+str(reqq.status_code)+"!")
                empl = json.loads(reqq.text)
                try:
                    # Получаем сферу деятельнотси работодателя 9 - телеком компании
                    if (empl["industries"][0]["id"] == 9):
                        logger.info("ID работодателя: "+str(empl["industries"][0]["id"])+"!")
                        position = vacancy["name"]
                        company_name = vacancy["employer"]["name"]
                        job_description = vacancy["description"]
                        key_skills = list()
                        for j in vacancy["key_skills"]:
                            key_skills.append(j["name"])
                        key_skills = str(key_skills)
                        rows = [(position,  company_name, job_description, key_skills),]
                        logger.info("Сформирован запрос в БД: "+str(rows)+"!")
                        fields = ['position', 'company_name', 'job_description', 'key_skills']
                        # Загрузка в БД
                        sqlite_hook.insert_rows(
                                table='vacancies',
                                rows=rows,
                                target_fields=fields,
                            )
                        time.sleep(0.1)
                    else:
                        continue
                except IndexError:
                        continue
            else:
                print("error")
                continue

#Функция рассчитывает топ 10 ключевых навыков
def count_keyskills():
    #Загружаем ключевые навыки из БД

    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_akh')
    sqlite_conn = sqlite_hook.get_conn()
    cur = sqlite_conn.cursor()
    res = cur.execute("SELECT key_skills FROM vacancies")
    arr = res.fetchall()

    #Рассчитываем топ 10
    result = []
    for n, x in enumerate(arr):
        array = []
        array = arr[n][0].replace("[","").replace("\'", "").replace("]", "")
        array = array.split(',')
        result = result + array

    # Выводим результат
    print(sorted(dict(Counter(result)).items(), key=lambda x: x[1])[-10:])

with DAG(
    dag_id='akhmetov_project',
    default_args=default_args,
    description='DAG',
    start_date=datetime(2023, 7, 15, 8),
    schedule_interval='@daily'
) as dag:
    #Загружаем файл egrul
    task1 = BashOperator(
            task_id='download_file',
            bash_command="wget https://ofdata.ru/open-data/download/egrul.json.zip -O /home/rtstudent/students/akhmetov/egrul.json.zip"
        )
    #Обрабатываем файлы
    task2 = PythonOperator(
        task_id='get_egrul',
        python_callable=get_egrul,
    )
    # Получаем вакансии
    task3 = PythonOperator(
        task_id='get_vacancy',
        python_callable=get_vacancy,
    )
    # Рассчитываем топ10 ключевых навыков
    task4 = PythonOperator(
        task_id='count_keyskills',
        python_callable=count_keyskills,
    )
    task1 >> task2 >> task3 >> task4
   