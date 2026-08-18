"""Worker launcher that registers a trivial benchmark handler, then runs dtq.worker."""
import sys
from dtq.worker.registry import task

@task(name="noop")
async def noop(payload: dict) -> dict:
    return {"ok": True}

from dtq.worker.__main__ import main
sys.argv = ["dtq.worker", "--queues", "bench-engine", "--concurrency", "32"]
main()
