from airflow import DAG 
from datetime import datetime 
from airflow.operators.bash import BashOperator


default_args = {
	'start_date': datetime(2020, 1, 1)
}

with DAG('all_done', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
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

	fire_when_all_3_done = BashOperator(
		task_id='fire_when_all_3_done',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_done"
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
		bash_command='exit 1',
		do_xcom_push=False,
	)

	fire_after_all_upstream_fail = BashOperator(
		task_id='fire_after_all_upstream_fail',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_done"
	)

	dummy_7 = BashOperator(
		task_id='dummy_7',
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

	wait_and_fire_when_all_upstream_done = BashOperator(
		task_id='wait_and_fire_when_all_upstream_done',
		bash_command='exit 0',
		do_xcom_push=False,
		trigger_rule="all_done"
	)

	( 
  	[task_1, task_2, task_3] >> fire_when_all_3_done >> [task_4, task_5, task_6] >> 
  	fire_after_all_upstream_fail >> dummy_7 >> [task_8, task_9, wait_and_fail] >> 
   	wait_and_fire_when_all_upstream_done
  )
