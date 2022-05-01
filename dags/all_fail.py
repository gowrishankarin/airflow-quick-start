from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('all_fail', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
	task_1 = BashOperator(
		task_id='task_1',
		bash_command='exit 1',
		do_xcom_push=False
	)

	task_2 = BashOperator(
		task_id='task_2',
		bash_command='exit 1',
		do_xcom_push=False
	)

	fire_after_1_2_fails = BashOperator(
		task_id='fire_after_1_2_fails',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_failed"
	)

	task_3 = BashOperator(
		task_id='task_3',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_4 = BashOperator(
		task_id='task_4',
		bash_command='exit 1',
		do_xcom_push=False
	)

	skip_since_3_succeeds = BashOperator(
		task_id='skip_since_3_succeeds',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_failed"
	)

	task_5 = BashOperator(
		task_id='task_5',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_6 = BashOperator(
		task_id='task_6',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_7 = BashOperator(
		task_id='task_7',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="dummy"
	)

	[task_1, task_2] >> fire_after_1_2_fails >> [task_3, task_4] >> skip_since_3_succeeds >> [task_5 >> task_7, task_6 >> task_7] 