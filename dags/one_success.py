from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('one_success', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
	task_1 = BashOperator(
		task_id='task_1',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_2 = BashOperator(
		task_id='task_2',
		bash_command='sleep 30 & exit 1',
		do_xcom_push=False
	)
 
	task_3 = BashOperator(
		task_id='task_3',
		bash_command='exit 1',
		do_xcom_push=False,
	)

	fire_when_1_succeeds = BashOperator(
		task_id='fire_when_1_succeeds',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="one_success"
	)

	task_4 = BashOperator(
		task_id='task_4',
		bash_command='exit 1',
		do_xcom_push=False
	)

	task_5 = BashOperator(
		task_id='task_5',
		bash_command='exit 1',
		do_xcom_push=False
	)
 
	task_6 = BashOperator(
		task_id='task_6',
		bash_command='sleep 30 & exit 0',
		do_xcom_push=False,
	)

	wait_for_task_6_to_succeed = BashOperator(
		task_id='wait_for_task_6_to_succeed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="one_success"
	)

	task_7 = BashOperator(
		task_id='task_7',
		bash_command='exit 0',
		do_xcom_push=False
	)

	( 
  	[task_1, task_2, task_3] >> fire_when_1_succeeds >> [task_4, task_5, task_6] >> 
  	wait_for_task_6_to_succeed >> task_7 
  )