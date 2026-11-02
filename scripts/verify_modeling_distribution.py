"""CHAOSS 2軸モデルの閾値を実データで検証するスクリプト (Issue #15)。

指定日の GH Archive を取り込み、リポジトリ単位の日次集計 (daily_stars/forks/issues/prs) を
メモリ上で作り、DS-001 第2.6節の暫定値が妥当かを測る。ディスクには何も書かない。

測定項目:
  1. 足切り条件 (daily_stars >= 3 かつ アクティビティ >= 1) を通る母集団の件数
  2. daily_stars と加重スコアの分布 (パーセンタイル) と、同順位の割合
  3. 0.80 境界で各象限に入る件数
  4. 3つの異常値フラグが、足切り前後でそれぞれ何件になるか
  5. 加重の候補 (Fork3:PR2:Issue1 と PR3:Issue2:Fork1) による Top 上位の違い

使い方:
  python scripts/verify_modeling_distribution.py 2024-01-02 --hours 0-23
  python scripts/verify_modeling_distribution.py 2024-01-02 --hours 0,6,12,18   # 動作確認用に間引く
注意: --hours を間引くと日次の値は過小になる。閾値の判断には全24時間を使うこと。
"""
from __future__ import annotations

import argparse
from collections import Counter
import datetime
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from octopulse.ingestion.stream_engine import extract_target_events, fetch_hourly_stream  # noqa: E402

# DS-001 第2.6節の暫定値
MIN_STARS = 3
MIN_ACTIVITY = 1
CUTOFF = 0.80
WEIGHTS_CURRENT = (3, 2, 1)  # (fork, pr, issue)
WEIGHTS_ISSUE6 = (1, 3, 2)  # Issue #6 の候補: PR3 : Issue2 : Fork1 -> (fork, pr, issue)


def parse_hours(spec: str) -> list[int]:
    hours: list[int] = []
    for part in spec.split(","):
        if "-" in part:
            start, end = part.split("-")
            hours.extend(range(int(start), int(end) + 1))
        else:
            hours.append(int(part))
    return sorted(set(hours))


def collect_day(target_date: datetime.date, hours: list[int]) -> dict[str, Counter[str]]:
    """リポジトリ単位で種別ごとの件数を数える (メモリ上のみ)。"""
    counters: dict[str, Counter[str]] = {k: Counter() for k in ("star", "fork", "issue", "pr")}
    key_by_type = {"WatchEvent": "star", "ForkEvent": "fork", "IssuesEvent": "issue", "PullRequestEvent": "pr"}
    for hour in hours:
        started = time.time()
        count = 0
        for event in extract_target_events(fetch_hourly_stream(target_date, hour)):
            counters[key_by_type[event["type"]]][event["repo"]["name"]] += 1
            count += 1
        print(f"  {target_date} {hour:02d}h: {count:,} events ({time.time() - started:.1f}s)", file=sys.stderr)
    return counters


def percent_rank(values: list[int]) -> dict[int, float]:
    """PERCENT_RANK() 相当: (自分より小さい値の件数) / (母集団 - 1)。同値は同順位。"""
    n = len(values)
    if n <= 1:
        return {v: 0.0 for v in values}
    ordered = sorted(values)
    first_index: dict[int, int] = {}
    for i, v in enumerate(ordered):
        first_index.setdefault(v, i)
    return {v: first_index[v] / (n - 1) for v in set(values)}


def quantiles(values: list[int], points: tuple[float, ...]) -> dict[str, int]:
    ordered = sorted(values)
    return {f"p{int(p * 100)}": ordered[min(int(p * (len(ordered) - 1)), len(ordered) - 1)] for p in points}


def summarize(target_date: datetime.date, counters: dict[str, Counter[str]]) -> dict[str, object]:
    repos = set().union(*(c.keys() for c in counters.values()))
    rows = {
        r: {
            "stars": counters["star"][r],
            "forks": counters["fork"][r],
            "issues": counters["issue"][r],
            "prs": counters["pr"][r],
        }
        for r in repos
    }

    def activity(v: dict[str, int]) -> int:
        return v["forks"] + v["issues"] + v["prs"]

    def score(v: dict[str, int], w: tuple[int, int, int]) -> int:
        return v["forks"] * w[0] + v["prs"] * w[1] + v["issues"] * w[2]

    population = {r: v for r, v in rows.items() if v["stars"] >= MIN_STARS and activity(v) >= MIN_ACTIVITY}
    result: dict[str, object] = {
        "date": target_date.isoformat(),
        "repos_total": len(rows),
        "repos_with_any_star": sum(1 for v in rows.values() if v["stars"] > 0),
        "repos_stars_ge3": sum(1 for v in rows.values() if v["stars"] >= MIN_STARS),
        "population": len(population),
    }
    if len(population) < 2:
        return result

    stars = [v["stars"] for v in population.values()]
    result["stars_quantiles"] = quantiles(stars, (0.25, 0.5, 0.75, 0.8, 0.9, 0.99))
    result["stars_tie_share_at_min"] = round(Counter(stars)[min(stars)] / len(stars), 3)
    result["stars_distinct_values"] = len(set(stars))

    for label, weights in (("current_F3_PR2_I1", WEIGHTS_CURRENT), ("issue6_F1_PR3_I2", WEIGHTS_ISSUE6)):
        scores = [score(v, weights) for v in population.values()]
        result[f"engagement_{label}"] = {
            "quantiles": quantiles(scores, (0.25, 0.5, 0.75, 0.8, 0.9, 0.99)),
            "tie_share_at_min": round(Counter(scores)[min(scores)] / len(scores), 3),
            "distinct_values": len(set(scores)),
        }

    # 象限分布 (現行の重み)
    attn = percent_rank(stars)
    eng_scores = {r: score(v, WEIGHTS_CURRENT) for r, v in population.items()}
    eng = percent_rank(list(eng_scores.values()))
    segments: Counter[str] = Counter()
    for r, v in population.items():
        a, e = attn[v["stars"]], eng[eng_scores[r]]
        if a >= CUTOFF and e >= CUTOFF:
            segments["Hyper-Growth"] += 1
        elif a >= CUTOFF:
            segments["Viral Spotlight"] += 1
        elif e >= CUTOFF:
            segments["Silent Utility"] += 1
        else:
            segments["Niche / Emerging"] += 1
    result["segments"] = dict(segments)
    # 0.80 以上に入る件数が、理論値 (20%) からどれだけずれるか (同順位の影響)
    attn_hits = sum(1 for v in population.values() if attn[v["stars"]] >= CUTOFF)
    eng_hits = sum(1 for r in population if eng[eng_scores[r]] >= CUTOFF)
    result["attention_ge_0.80_share"] = round(attn_hits / len(population), 3)
    result["engagement_ge_0.80_share"] = round(eng_hits / len(population), 3)

    # フラグ: 足切り前 (全リポジトリ) と足切り後 (母集団) の件数
    def flags(src: dict[str, dict[str, int]]) -> dict[str, int]:
        return {
            "is_suspicious_star_surge": sum(1 for v in src.values() if v["stars"] >= 10 and v["forks"] + v["prs"] == 0),
            "is_bot_generated_prs": sum(1 for v in src.values() if v["prs"] >= 50 and v["stars"] == 0),
        }

    result["flags_before_cutoff"] = flags(rows)
    result["flags_in_population"] = flags(population)
    result["top5_by_stars"] = [
        {"repo": r, **v} for r, v in sorted(population.items(), key=lambda kv: -kv[1]["stars"])[:5]
    ]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("date", type=datetime.date.fromisoformat, nargs="+", help="対象日 (YYYY-MM-DD)。複数指定可")
    parser.add_argument("--hours", default="0-23", help="対象時間 (例: 0-23 / 0,6,12,18)")
    parser.add_argument("--output", type=Path, help="集計結果(JSON)の保存先。省略時は標準出力のみ")
    args = parser.parse_args()
    hours = parse_hours(args.hours)

    results = []
    for target_date in args.date:
        print(f"=== {target_date} hours={hours} ===", file=sys.stderr)
        results.append(summarize(target_date, collect_day(target_date, hours)))
    text = json.dumps(results, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8", newline="\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
