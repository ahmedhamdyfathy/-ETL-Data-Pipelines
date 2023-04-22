# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'ahmed hamdy',
    'start_date': days_ago(0),
    'email': ['ahmed@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks
# define the first task
unzip_data = BashOperator(
    task_id='unzip-data',
    bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

# define the task 'extract data from csv'

extract_data_from_csv = BashOperator(
    task_id='extract-data-from-csv',
    bash_command='cut -d "," -f 1,2,3,4  /home/project/dvehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# define the task 'extract data from tsv file'

extract_data_from_tsv = BashOperator(
    task_id='extract-data-from-tsv',
    bash_command='cut -d " " -f 5,6,7 /home/project/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

# define the task 'extract data from fixed width file'

extract_data_from_fixed_width = BashOperator(
    task_id='extract-data-from-fixed-width',
    bash_command='cut -d" " -f 6,7  /home/project/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag,
)

# define the task 'consolidate data extracted from previous tasks'

consolidate_data = BashOperator(
    task_id='consolidate-data',
    bash_command='paste /home/project//airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag,
)

# define the task 'transform and load the data'

transform_data = BashOperator(
    task_id='transform-data',
    bash_command='tr [a-z] [A-Z] < /home/project/airflow/dags/extracted_data.csv  > /home/project/airflow/dags/transformed_data.csv',
    dag=dag,
)

# define the task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data