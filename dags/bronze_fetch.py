"""DAG 2: execute the plan written by `bronze_plan` (triggered with `{"run_id": ...}`).

Filings are fetched in a single sequential task on purpose: the token bucket lives in process
memory, so parallel tasks would each get their own limiter and multiply the request rate.

The later tasks use `all_done` so the summary and validation report are written even when some
filings failed; the failed `fetch_filings` task still leaves the DAG run red.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task


def _run_id() -> str:
    conf = get_current_context()["dag_run"].conf or {}
    if "run_id" not in conf:
        raise ValueError("trigger bronze_fetch with conf {'run_id': '<plan run_id>'}")
    return str(conf["run_id"])


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "sec"],
)
def bronze_fetch():
    @task
    def load_plan() -> dict:
        """Fail fast if the plan is missing or incomplete, and log what is about to happen."""
        from sec_ingest import pipeline, plan

        pipeline.configure_logging()
        _, summary = plan.read_plan(pipeline.open_runtime().store, _run_id())
        return summary

    @task
    def fetch_filings() -> None:
        from sec_ingest import pipeline

        pipeline.configure_logging()
        pipeline.fetch_filings_stage(pipeline.open_runtime(), _run_id())

    @task(trigger_rule="all_done")
    def fetch_companyfacts() -> None:
        from sec_ingest import pipeline

        pipeline.configure_logging()
        pipeline.companyfacts_stage(pipeline.open_runtime(), _run_id())

    @task(trigger_rule="all_done")
    def validate() -> None:
        from sec_ingest import pipeline

        report = pipeline.validate_stage(pipeline.open_runtime(), _run_id())
        for warning in report.warnings:
            print(f"WARNING {warning}")
        if not report.ok:
            raise RuntimeError("Bronze validation failed:\n" + "\n".join(report.errors))

    @task(trigger_rule="all_done")
    def write_fetch_summary() -> dict:
        from sec_ingest import pipeline

        return pipeline.summarize_stage(pipeline.open_runtime(), _run_id())

    load_plan() >> fetch_filings() >> fetch_companyfacts() >> validate() >> write_fetch_summary()


bronze_fetch()
