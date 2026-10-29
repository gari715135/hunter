# ./src/prometheus_tools/orchestrator.py
import contextvars
import itertools
import logging
import signal
import threading
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from src.logger_config import log_context
from src.prometheus_tools.prom_ingestor import JobResult, PromDataIngester
from src.prometheus_tools.utils import elapsed_ms

# Named tuple for job structure: (metric, cluster_or_none)
Job = namedtuple("Job", ["metric", "cluster"])

# Global flag for graceful shutdown
shutdown_requested = False
shutdown_signal: Optional[int] = None

_signal_logger = logging.getLogger("prometheus_pipeline.orchestrator")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested, shutdown_signal
    shutdown_requested = True
    shutdown_signal = signum
    _signal_logger.warning(
        "Shutdown requested, completing in-flight jobs",
        extra={"event": "shutdown_requested", "signal": signal.Signals(signum).name},
    )


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def job_id(job: Job) -> str:
    return f"{job.metric}:{job.cluster or 'all_clusters'}"


class ProgressTracker:
    """Thread-safe progress tracker with periodic logging."""

    def __init__(self, total: int, logger, log_interval: int = 10):
        self.total = total
        self.completed = 0
        self.succeeded = 0
        self.empty = 0
        self.failed = 0
        self.truncated = 0  # jobs where Sysdig capped the series returned (data incomplete)
        self.failed_jobs: List[str] = []
        self.logger = logger
        self.lock = threading.Lock()
        self.start_time = time.monotonic()
        self.log_interval = log_interval  # Log every N completions

    def record(self, job_name: str, status: str, truncated: bool = False):
        """Record a finished job (status: success | empty | failed)."""
        with self.lock:
            self.completed += 1
            self.truncated += truncated
            if status == "success":
                self.succeeded += 1
            elif status == "empty":
                self.empty += 1
            else:
                self.failed += 1
                self.failed_jobs.append(job_name)

            # Log at intervals or on completion
            if self.completed % self.log_interval == 0 or self.completed == self.total:
                self._log_progress()

    def _log_progress(self):
        """Log current progress."""
        elapsed = time.monotonic() - self.start_time
        rate = self.completed / elapsed if elapsed > 0 else 0
        remaining = self.total - self.completed
        eta = remaining / rate if rate > 0 else 0

        self.logger.info(
            f"Progress {self.completed}/{self.total}",
            extra={
                "event": "fetch_progress",
                "completed": self.completed,
                "total": self.total,
                "percent": round(self.completed / self.total * 100, 1),
                "failed": self.failed,
                "rate_per_s": round(rate, 2),
                "elapsed_ms": int(elapsed * 1000),
                "eta_s": round(eta, 1),
            },
        )

    def get_summary(self) -> dict:
        """Get final summary statistics."""
        elapsed = time.monotonic() - self.start_time
        return {
            "total": self.total,
            "completed": self.completed,
            "not_run": self.total - self.completed,
            "succeeded": self.succeeded,
            "empty": self.empty,
            "failed": self.failed,
            "truncated": self.truncated,
            "failed_jobs": sorted(self.failed_jobs),
            "success_rate": (
                round((self.succeeded + self.empty) / self.total * 100, 1) if self.total > 0 else 0
            ),
            "execution_ms": int(elapsed * 1000),
            "avg_rate_per_s": round(self.completed / elapsed, 2) if elapsed > 0 else 0,
        }


class MetricsFetchOrchestrator:
    """Orchestrates parallel fetching of metrics from Prometheus."""

    def __init__(self, ingestor: PromDataIngester, logger=None):
        self.ingestor = ingestor
        self.logger = logger or ingestor.logger

    def create_jobs(
        self,
        metrics: List[str],
        clusters: List[str],
        fetch_all_clusters: bool = False,
    ) -> List[Job]:
        """Create list of fetch jobs based on mode."""
        if not metrics:
            self.logger.error("Metric list is empty", extra={"event": "no_metrics"})
            raise ValueError("Metric list is empty")

        if fetch_all_clusters:
            self.logger.info(
                "Creating jobs, one query per metric",
                extra={
                    "event": "jobs_created",
                    "mode": "all_clusters",
                    "metrics": len(metrics),
                    "jobs": len(metrics),
                },
            )
            return [Job(metric=m, cluster=None) for m in metrics]
        else:
            if not clusters:
                self.logger.error("Cluster list is empty", extra={"event": "no_clusters"})
                raise ValueError("Cluster list is empty")
            self.logger.info(
                "Creating jobs, one query per metric and cluster",
                extra={
                    "event": "jobs_created",
                    "mode": "per_cluster",
                    "metrics": len(metrics),
                    "clusters": len(clusters),
                    "jobs": len(metrics) * len(clusters),
                },
            )
            return [Job(metric=m, cluster=c) for m, c in itertools.product(metrics, clusters)]

    def _log_job_completed(self, result: JobResult, duration_ms: int) -> None:
        level = logging.ERROR if result.status == "failed" else logging.INFO
        self.logger.log(
            level,
            f"Fetch job {result.status}",
            extra={
                "event": "fetch_job_completed",
                **result.as_fields(),
                **self.ingestor.window_fields(),
                "duration_ms": duration_ms,
            },
        )

    def _run_job(self, job: Job, fetch_all_clusters: bool) -> Optional[JobResult]:
        """Run one job inside its own log context; never raises."""
        # Check shutdown before starting work
        if shutdown_requested:
            return None

        with log_context(
            job_id=job_id(job), metric=job.metric, cluster=job.cluster or "all_clusters"
        ):
            start_time = time.monotonic()
            try:
                result = self.ingestor.get_sysdig_metric(
                    job.metric,
                    job.cluster,
                    fetch_all_clusters=fetch_all_clusters,
                )
            except Exception as e:
                self.logger.exception(
                    "Fetch job raised an exception", extra={"event": "fetch_job_error"}
                )
                result = JobResult(
                    metric=job.metric,
                    cluster=job.cluster or "all_clusters",
                    status="failed",
                    error_type=type(e).__name__,
                    error=str(e),
                )
            self._log_job_completed(result, elapsed_ms(start_time))
            return result

    def execute_parallel(
        self,
        jobs: List[Job],
        workers: int,
        fetch_all_clusters: bool = False,
    ) -> dict:
        """Execute fetch jobs in parallel with progress tracking. Returns the run summary."""
        # Initialize progress tracker
        log_interval = max(1, min(10, len(jobs) // 20))
        progress = ProgressTracker(len(jobs), self.logger, log_interval=log_interval)

        self.logger.info(
            "Starting parallel execution",
            extra={
                "event": "fetch_execution_started",
                "total_jobs": len(jobs),
                "workers": workers,
                "log_interval": log_interval,
            },
        )

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
            # Each job runs in a copy of this thread's context, so the run_id follows it into the worker
            futures = {
                pool.submit(
                    contextvars.copy_context().run, self._run_job, job, fetch_all_clusters
                ): job
                for job in jobs
            }

            for future in as_completed(futures):
                job = futures[future]
                try:
                    result = future.result()
                except Exception as e:  # _run_job catches everything; this is a last resort
                    self.logger.exception(
                        "Fetch job crashed",
                        extra={
                            "event": "fetch_job_error",
                            "job_id": job_id(job),
                            "metric": job.metric,
                            "cluster": job.cluster,
                        },
                    )
                    result = JobResult(
                        metric=job.metric,
                        cluster=job.cluster or "all_clusters",
                        status="failed",
                        error_type=type(e).__name__,
                        error=str(e),
                    )

                # None means the job was skipped because shutdown was requested
                if result is not None:
                    progress.record(job_id(job), result.status, truncated=result.truncated)

                if shutdown_requested:
                    self.logger.warning(
                        "Shutdown requested, cancelling remaining jobs",
                        extra={"event": "jobs_cancelling"},
                    )
                    # Cancel remaining futures
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    # Break out of loop to allow executor context manager to cleanup
                    break

        summary = progress.get_summary()
        summary["interrupted"] = shutdown_requested
        summary["signal"] = signal.Signals(shutdown_signal).name if shutdown_signal else None
        return summary

    def run_fetch_cycle(
        self,
        metrics: List[str],
        clusters: List[str],
        workers: int,
        fetch_all_clusters: bool = False,
    ) -> dict:
        """Run complete fetch cycle: create jobs and execute in parallel. Returns the run summary."""
        jobs = self.create_jobs(metrics, clusters, fetch_all_clusters)
        return self.execute_parallel(jobs, workers, fetch_all_clusters)
