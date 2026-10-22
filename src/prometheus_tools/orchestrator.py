# ./src/prometheus_tools/orchestrator.py
import itertools
import signal
import sys
import threading
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from src.prometheus_tools.prom_ingestor import PromDataIngester

# Named tuple for job structure: (metric, cluster_or_none)
Job = namedtuple("Job", ["metric", "cluster"])

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested
    print("\nShutdown requested. Completing current tasks...")
    shutdown_requested = True


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class ProgressTracker:
    """Thread-safe progress tracker with periodic logging."""

    def __init__(self, total: int, logger, log_interval: int = 10):
        self.total = total
        self.completed = 0
        self.failed = 0
        self.logger = logger
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.log_interval = log_interval  # Log every N completions

    def increment(self, success: bool = True):
        """Increment progress counter."""
        with self.lock:
            self.completed += 1
            if not success:
                self.failed += 1

            # Log at intervals or on completion
            if self.completed % self.log_interval == 0 or self.completed == self.total:
                self._log_progress()

    def _log_progress(self):
        """Log current progress."""
        elapsed = time.time() - self.start_time
        percent = (self.completed / self.total) * 100
        rate = self.completed / elapsed if elapsed > 0 else 0

        remaining = self.total - self.completed
        eta = remaining / rate if rate > 0 else 0

        self.logger.info(
            f"Progress: {self.completed}/{self.total} ({percent:.1f}%) | "
            f"Failed: {self.failed} | "
            f"Rate: {rate:.2f} queries/sec | "
            f"Elapsed: {elapsed:.1f}s | "
            f"ETA: {eta:.1f}s"
        )

    def get_summary(self) -> dict:
        """Get final summary statistics."""
        elapsed = time.time() - self.start_time
        return {
            "total": self.total,
            "completed": self.completed,
            "failed": self.failed,
            "success_rate": ((self.completed - self.failed) / self.total * 100) if self.total > 0 else 0,
            "elapsed_seconds": elapsed,
            "avg_rate": self.completed / elapsed if elapsed > 0 else 0,
        }


class MetricsFetchOrchestrator:
    """Orchestrates parallel fetching of metrics from Prometheus."""

    def __init__(self, ingestor: PromDataIngester):
        self.ingestor = ingestor
        self.logger = ingestor.logger

    def create_jobs(
        self,
        metrics: List[str],
        clusters: List[str],
        fetch_all_clusters: bool = False,
    ) -> List[Job]:
        """Create list of fetch jobs based on mode."""
        if not metrics:
            self.logger.error("Metric list is empty")
            raise ValueError("Metric list is empty")

        if fetch_all_clusters:
            self.logger.info("Creating jobs for FETCH_ALL_CLUSTERS mode - one query per metric")
            return [Job(metric=m, cluster=None) for m in metrics]
        else:
            if not clusters:
                self.logger.error("Cluster list is empty")
                raise ValueError("Cluster list is empty")
            self.logger.info(f"Creating jobs for PER_CLUSTER mode - {len(metrics)} metrics x {len(clusters)} clusters")
            return [Job(metric=m, cluster=c) for m, c in itertools.product(metrics, clusters)]

    def execute_parallel(
        self,
        jobs: List[Job],
        workers: int,
        fetch_all_clusters: bool = False,
    ) -> None:
        """Execute fetch jobs in parallel with progress tracking."""

        # Capture correlation_id from current thread context
        from src.logger_config import _context

        correlation_id = getattr(_context, "correlation_id", "none")

        # Initialize progress tracker
        log_interval = max(1, min(10, len(jobs) // 20))
        progress = ProgressTracker(len(jobs), self.logger, log_interval=log_interval)

        self.logger.info(
            f"Starting parallel execution total_jobs={len(jobs)} workers={workers} " f"log_interval={log_interval}"
        )

        def execute_job_with_context(job, corr_id):
            """Wrapper that sets context before executing job."""
            # Check shutdown before starting work
            if shutdown_requested:
                return None

            # Set correlation ID for this worker thread
            _context.correlation_id = corr_id
            return self.ingestor.get_sysdig_metric(
                job.metric,
                job.cluster,
                fetch_all_clusters=fetch_all_clusters,
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(execute_job_with_context, job, correlation_id) for job in jobs]

            for future in as_completed(futures):
                if shutdown_requested:
                    self.logger.warning("Shutdown requested, cancelling remaining tasks...")
                    # Cancel remaining futures
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    # Break out of loop to allow executor context manager to cleanup
                    break

                success = True
                try:
                    result = future.result()
                    # Skip cancelled tasks
                    if result is None and shutdown_requested:
                        continue
                except Exception as e:
                    success = False
                    self.logger.error(f"Task failed with error: {e}")

                progress.increment(success=success)

        # This will be reached after executor cleanup
        if shutdown_requested:
            self.logger.warning("Execution interrupted by user")
            summary = progress.get_summary()
            self.logger.info(
                f"Partial execution summary total={summary['total']} "
                f"completed={summary['completed']} failed={summary['failed']} "
                f"success_rate={summary['success_rate']:.1f}% "
                f"elapsed={summary['elapsed_seconds']:.1f}s"
            )
            sys.exit(130)  # Standard exit code for Ctrl+C

        # Log final summary (normal completion)
        summary = progress.get_summary()
        self.logger.info(
            f"Execution completed total={summary['total']} "
            f"completed={summary['completed']} failed={summary['failed']} "
            f"success_rate={summary['success_rate']:.1f}% "
            f"elapsed={summary['elapsed_seconds']:.1f}s "
            f"avg_rate={summary['avg_rate']:.2f} queries/sec"
        )

    def run_fetch_cycle(
        self,
        metrics: List[str],
        clusters: List[str],
        workers: int,
        fetch_all_clusters: bool = False,
    ) -> None:
        """Run complete fetch cycle: create jobs and execute in parallel."""
        jobs = self.create_jobs(metrics, clusters, fetch_all_clusters)
        self.execute_parallel(jobs, workers, fetch_all_clusters)
        