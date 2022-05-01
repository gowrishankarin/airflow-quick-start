from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator

default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('none_skipped', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
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

	fire_since_3_failed = BashOperator(
		task_id='fire_since_2_failed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="one_failed"
	)

	task_4 = BashOperator(
		task_id='task_4',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_5 = BashOperator(
		task_id='task_5',
		bash_command='exit 0',
		do_xcom_push=False
	)
 
	task_6 = BashOperator(
		task_id='task_6',
		bash_command='exit 0',
		do_xcom_push=False,
	)

	skip_since_all_upstream_succeeded = BashOperator(
		task_id='skip_since_all_upstream_succeeded',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="one_failed"
	)

	do_not_proceed = BashOperator(
		task_id='do_not_proceed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="dummy"
	)
 
	task_8 = BashOperator(
		task_id='task_8',
		bash_command='exit 0',
		do_xcom_push=False
	)

	task_9 = BashOperator(
		task_id='task_9',
		bash_command='exit 1',
		do_xcom_push=False
	)
 
	wait_and_fail = BashOperator(
		task_id='wait_and_fail',
		bash_command='sleep 30 & exit 1',
		do_xcom_push=False,
	)

	skip_since_2_upstream_failed = BashOperator(
		task_id='skip_since_2_upstream_failed',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="one_failed"
	)

	( 
  	[task_1, task_2, task_3] >> fire_since_3_failed >> [task_4, task_5, task_6] >> 
  	skip_since_all_upstream_succeeded >> do_not_proceed >> [task_8, task_9, wait_and_fail] >> 
   	skip_since_2_upstream_failed
  )