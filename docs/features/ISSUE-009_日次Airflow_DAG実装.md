---
title: "課題記録: 日次Airflow DAGおよびGCSアップロード実装"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #9 の日次Airflow DAGおよびGCSストリーミングアップロード実装をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/9"
snapshot_at: "2026-11-01"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
---

# 課題記録
**[Ingestion] 日次Airflow DAGおよびGCSストリーミングアップロードの実装**

| 項目         | 内容                                                                     |
| :----------- | :----------------------------------------------------------------------- |
| Issue番号    | #9                                                                       |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/9                  |
| 状態         | Open                                                                     |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. Issue本文・概要

- **目的**:
  - `DD-001` で規定された日次Ingestionパイプライン（`run_daily_ingestion`）を完成させ、GCSへのダイレクトストリーミングアップロードを実装する。
  - Apache Airflow による日次スケジューリングDAG（`gh_stream_ingest.py`）およびローカル開発用 `docker-compose.yaml`（ADCマウント構成）を整備する。
- **対象範囲**:
  - GCSストリーミングアップロードモジュール (`src/octopulse/ingestion/gcs_uploader.py`)
  - 日次Airflow DAG (`airflow/dags/gh_stream_ingest.py`)
  - ローカル実行環境 (`airflow/docker-compose.yaml`, `airflow/.env.example`)
  - 単体テスト (`tests/test_gcs_uploader.py`)

## 2. 実装および決定事項

- **ゼロディスクI/O契約の遵守**:
  - `google.cloud.storage.Client` の `blob.open("w")` を利用し、24時間分の抽出JSONLをディスクに書き込まず直接GCSへストリーミング送出する。
- **Airflowオーケストレーション**:
  - `@daily` スケジュールにより、対象日付（`{{ ds }}`）の24ファイルを逐次処理。
  - 冪等性（Idempotency）の担保として、同日付の再実行時は同一GCSオブジェクトパスへ上書き（Overwrite）する。
- **ADCマウントのセキュア設定**:
  - ホスト上の `~/.config/gcloud` を `:ro` でマウントし、固定キーを発行・保持しない運用を担保。

## 3. 関連文書

- **基本設計書**: [`docs/design/BD-001_基本設計書.md`](../design/BD-001_基本設計書.md)
- **Ingestion詳細設計書**: [`docs/design/DD-001_Ingestionエンジン詳細設計書.md`](../design/DD-001_Ingestionエンジン詳細設計書.md)
- **データ構造仕様書**: [`docs/design/DS-001_データ構造仕様書.md`](../design/DS-001_データ構造仕様書.md)
