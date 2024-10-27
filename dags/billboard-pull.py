from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime

from billboard_charts import billboard


def download_chart(**context):
    
    if context['dag_run'].conf.get('run_date') is not None:
        chart_date = context['dag_run'].conf.get('run_date')
    
    elif context['dag'].catchup:
        chart_date = context['execution_date'].strftime('%Y-%m-%d')
    
    else:
        chart_date = datetime.now().strftime('%Y-%m-%d') 
        
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
            }
    
    chart = billboard.ChartData("artist-100", date=chart_date, headers=headers)

    mysql_hook = MySqlHook(mysql_conn_id='music-db')
    dbconn = mysql_hook.get_conn()
    cursor = dbconn.cursor()

    cursor.callproc('add_chart', ["artist-100", chart_date])
    chart_id = cursor.fetchone()[0]
    cursor.nextset()

    for entry in chart:
        cursor.callproc('add_chart_artist', [chart_id, entry.artist, entry.rank, entry.weeks])
        cursor.nextset() 
    dbconn.commit()
    cursor.close()
    dbconn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 9),  # Start backfilling for first full chart week of 2024
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['ksmeeks0001@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'billboard_chart',
    default_args=default_args,
    description='Download weekly billboard chart',
    schedule_interval='0 11 * * 2',  # Every Tuesday at 11AM
    catchup=True  # Enable catchup to allow backfilling
) as dag:

    download_chart_task = PythonOperator(
        task_id='download_chart',
        python_callable=download_chart,
        provide_context=True,
    )

    download_chart_task

