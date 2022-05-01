from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('all_success.py', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
	task_1 = BashOperator(
		task_id='task_1',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_2 = BashOperator(
		task_id='task_2',
		bash_command='sleep 30 & exit 0',
		do_xcom_push=False
	)
 
	task_3 = BashOperator(
		task_id='task_3',
		bash_command='exit 0',
		do_xcom_push=False,
	)

	fire_when_all_succeeds = BashOperator(
		task_id='fire_when_all_succeeds',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_success"
	)

	task_4 = BashOperator(
		task_id='task_4',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_5 = BashOperator(
		task_id='task_5',
		bash_command='exit 1',
		do_xcom_push=False
	)
 
	task_6 = BashOperator(
		task_id='task_6',
		bash_command='exit 0',
		do_xcom_push=False,
	)

	skip_since_5_failed = BashOperator(
		task_id='skip_since_5_failed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_success"
	)

	task_7 = BashOperator(
		task_id='task_7',
		bash_command='exit 0',
		do_xcom_push=False
	)
 
	( 
  	[task_1, task_2, task_3] >> fire_when_all_succeeds >> [task_4, task_5, task_6] >> 
  	skip_since_5_failed >> task_7 # >> [task_8, task_9, wait_and_fail] >> 
   
  )