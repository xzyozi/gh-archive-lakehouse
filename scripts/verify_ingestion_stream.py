"""Verification script for GH Archive In-flight Streaming and Event Extraction.

Measures memory usage, event extraction counts, and top repos without local disk writes.
"""

from __future__ import annotations

import datetime
import sys
import time
import tracemalloc
from collections import Counter
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from octopulse.ingestion.stream_engine import fetch_hourly_stream, extract_target_events


def run_verification(target_date: datetime.date, hour: int, limit_events: int | None = None):
    print(f"=== Starting Ingestion Verification for {target_date} Hour {hour} ===")
    
    tracemalloc.start()
    start_time = time.time()
    
    byte_stream = fetch_hourly_stream(target_date, hour, chunk_size=65536)
    event_counts = Counter()
    star_by_repo = Counter()
    fork_by_repo = Counter()
    pr_by_repo = Counter()
    issue_by_repo = Counter()
    
    total_extracted = 0
    
    try:
        for event in extract_target_events(byte_stream):
            total_extracted += 1
            event_type = event.get("type", "Unknown")
            event_counts[event_type] += 1
            
            repo_name = event.get("repo", {}).get("name")
            if repo_name:
                if event_type == "WatchEvent":
                    star_by_repo[repo_name] += 1
                elif event_type == "ForkEvent":
                    fork_by_repo[repo_name] += 1
                elif event_type == "PullRequestEvent":
                    pr_by_repo[repo_name] += 1
                elif event_type == "IssuesEvent":
                    issue_by_repo[repo_name] += 1
            
            if limit_events and total_extracted >= limit_events:
                print(f"Reached event limit ({limit_events}). Stopping stream.")
                break
            
            if total_extracted % 20000 == 0:
                current, peak = tracemalloc.get_traced_memory()
                elapsed = time.time() - start_time
                print(f"  Processed {total_extracted:,} events ({elapsed:.1f}s) - Peak RAM: {peak / 1024 / 1024:.2f} MB")
                
    except Exception as exc:
        print(f"Error during streaming: {exc}")
        raise
        
    elapsed_total = time.time() - start_time
    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    print("\n=== Verification Results ===")
    print(f"Total Target Events Extracted: {total_extracted:,}")
    print(f"Elapsed Time: {elapsed_total:.2f} seconds")
    print(f"Peak Memory Usage: {peak_mem / 1024 / 1024:.2f} MB")
    print(f"Target Events Breakdown:")
    for event_type, count in event_counts.most_common():
        percentage = (count / total_extracted * 100) if total_extracted > 0 else 0
        print(f"  - {event_type:<18}: {count:>7,} ({percentage:5.1f}%)")
        
    print(f"\nUnique Repositories with Activity: {len(set(star_by_repo.keys()) | set(fork_by_repo.keys())):,}")
    print("\nTop 10 Repositories by Star (WatchEvent) in this hour:")
    for rank, (repo, count) in enumerate(star_by_repo.most_common(10), 1):
        print(f"  {rank:>2}. {repo:<45} : +{count} stars (Forks: {fork_by_repo[repo]}, PRs: {pr_by_repo[repo]})")

    print("\nTop 5 Repositories by PR (PullRequestEvent) in this hour:")
    for rank, (repo, count) in enumerate(pr_by_repo.most_common(5), 1):
        print(f"  {rank:>2}. {repo:<45} : {count} PRs (Stars: {star_by_repo[repo]})")


if __name__ == "__main__":
    # Test with 2024-01-01 15:00 UTC (1 hour of real GH Archive data)
    test_date = datetime.date(2024, 1, 1)
    test_hour = 15
    run_verification(test_date, test_hour)
