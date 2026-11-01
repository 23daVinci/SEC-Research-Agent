"""DAG 1: decide what to fetch and write an immutable plan, then trigger `bronze_fetch`.

All logic lives in `sec_ingest`; this file only wires it into Airflow. The plan steps (ticker map,
CIK resolution, submissions, coverage guard, selection, ledger diff, plan files) run in one task
because they share one in-process rate limiter and the plan must be written exactly once.

Deliberately unscheduled (`schedule=None`): run it manually or trigger it programmatically.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # two planners racing would double the request rate and read stale ledgers
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "sec"],
)
def bronze_plan():
    @task
    def build_plan() -> str:
        from sec_ingest import layout, pipeline

        pipeline.configure_logging()
        # Derived from the DAG run, so a task retry reuses the same run_id and plan.
        dag_run = get_current_context()["dag_run"]
        run_id = layout.ts_for_key(dag_run.run_after)
        # Optional trigger conf, e.g. {"universe": "sp500", "limit": 25}: pick the company source
        # (default from config) and plan only the first N companies (staged rollout).
        conf = dag_run.conf or {}
        limit = conf.get("limit")
        pipeline.run_plan_stage(
            pipeline.open_runtime(),
            run_id=run_id,
            universe=conf.get("universe"),
            limit=int(limit) if limit else None,
        )
        return run_id

    plan_run_id = "{{ ti.xcom_pull(task_ids='build_plan') }}"
    trigger_fetch = TriggerDagRunOperator(
        task_id="trigger_fetch",
        trigger_dag_id="bronze_fetch",
        trigger_run_id=f"bronze_fetch__{plan_run_id}",  # one fetch run per plan
        conf={"run_id": plan_run_id},
        skip_when_already_exists=True,
        wait_for_completion=False,
    )
    build_plan() >> trigger_fetch


bronze_plan()
