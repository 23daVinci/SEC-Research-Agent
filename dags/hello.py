"""Smoke-test DAG: verifies the stack runs tasks and that logs reach Loki at every level.

`failing_task` fails on purpose so there is an error-level log to look for in Grafana.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["smoke"])
def hello():
    @task
    def say_hello() -> str:
        log.debug("debug message")
        log.info("hello from the task")
        log.warning("this is a warning")
        return "ok"

    @task(retries=0)
    def failing_task():
        log.error("about to fail on purpose")
        raise RuntimeError("intentional failure for log testing")

    say_hello() >> failing_task()


hello()
