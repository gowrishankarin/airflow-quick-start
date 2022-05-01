from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('none_failed', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
	task_1 = BashOperator(
		task_id='task_1',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_2 = BashOperator(
		task_id='task_2',
		bash_command='exit 1',
		do_xcom_push=False
	)
 
	task_3 = BashOperator(
		task_id='task_3',
		bash_command='exit 0',
		do_xcom_push=False,
	)

	skip_since_2_failed = BashOperator(
		task_id='skip_since_2_failed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="none_failed"
	)

	task_4 = BashOperator(
		task_id='task_4',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="dummy"
	)

	( 
  	[task_1, task_2, task_3] >> skip_since_2_failed >> task_4
  )