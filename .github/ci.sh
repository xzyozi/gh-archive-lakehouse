#!/usr/bin/env bash
# プロジェクトCI契約: .github/workflows/ci.yml から呼び出される。
# ruff / mypy / pytest を実行し、いずれかが失敗したら非ゼロで終了する。
# CIランナーは使い捨てのため、仮想環境は作らず現在のPythonへ導入する。
# ローカルで実行する場合は、仮想環境を有効化してから実行すること。
set -euo pipefail

cd "$(dirname "$0")/.."

# ubuntu-latest には python が無く python3 のみの場合があるため両方を探す
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"

"$PYTHON" -m pip install --quiet -e ".[dev]"

echo "== ruff =="
"$PYTHON" -m ruff check .

echo "== mypy =="
"$PYTHON" -m mypy src

echo "== pytest =="
"$PYTHON" -m pytest -q

if command -v terraform >/dev/null 2>&1; then
  echo "== terraform fmt =="
  terraform fmt -check -recursive terraform/
fi
