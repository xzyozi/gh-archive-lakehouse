---
title: "課題記録: リージョン決定とAirflowイメージ"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
version: "1.0"
created_at: "2026-11-02"
updated_at: "2026-11-02"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #16 のリージョン決定と Airflow 自作イメージ導入をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/16"
snapshot_at: "2026-11-02"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "Airflow 自作イメージ手順書（相対パス: ../setup/airflow_image.md）"
---

# 課題記録
**リージョン決定とAirflowイメージ（Issue #16）**

| 項目         | 内容                                                    |
| :----------- | :------------------------------------------------------ |
| Issue番号    | #16                                                     |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/16 |
| 状態         | 実装中（PR作成待ち）                                    |
| 作成者       | xzyozi                                                  |
| 担当者       | xzyozi                                                  |
| 記録日時     | 2026-11-02                                              |
| 最終確認日時 | 2026-11-02                                              |

## 1. Issue本文・概要

- **目的・背景**:
  - GCS の Always Free 枠は `us-central1` / `us-east1` / `us-west1` のみが対象で、マルチリージョン `US` は対象外。具体的なリージョンを決める。
  - 現行の `airflow/docker-compose.yaml` は `apache/airflow:2.9.1-python3.11` をそのまま使い、`dbt-bigquery` と `astronomer-cosmos` を含まない。`BD-001` の Cosmos 経由 dbt 実行を満たす自作イメージが必要。
- **対象範囲**: GCS・BigQuery のリージョン決定、Airflow 自作 Dockerfile、compose の build 化、関連文書。
- **対象外**: dbt プロジェクトと DAG の実装、Terraform によるリソース作成。

## 2. 議論・決定・対応

- **主な論点**: Always Free 対象リージョンの選択、Airflow と dbt の依存衝突の回避。
- **決定事項**:
  - GCS バケットと BigQuery データセットは `us-central1` に統一する。
  - Airflow 本体に `astronomer-cosmos`、専用 venv `/opt/airflow/dbt_venv` に `dbt-bigquery` を導入する。版は PyPI 公開から3日以上経過したものを固定する（詳細は手順書）。
- **対応状況**: 対応中（文書・Dockerfile・compose を更新済み。Docker ビルドは未検証）。
- **次のアクション**: ビルドと導入確認の実施、PR 作成。

## 3. 関連文書

- **設計書**: [BD-001 基本設計書](../design/BD-001_基本設計書.md) 3.2節（Rev.1.4）
- **手順書**: [Airflow 自作イメージ](../setup/airflow_image.md)
- **実装**: `airflow/Dockerfile`、`airflow/docker-compose.yaml`、`airflow/.dockerignore`

## 4. 分析・知見（必要な場合のみ）

- **確認した事実**: `docker-compose.yaml` の `_AIRFLOW_WWW_USER_PASSWORD` 行末に余分な文字列（`IRFLOW_WWW_USER_PASSWORD in airflow/.env}`）が残っていた。パスワードの値が壊れるため、本件で修正した。
- **昇格先**: なし

## 5. 記録上の注意

- [x] Issue URLと取得日時を記録した。
- [x] 決定事項に関連する正本文書へのリンクを設定した。
- [x] 機密情報、認証情報、利用者固有の情報を転記していない。

## 6. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)  |
| :------ | :--------- | :----- | :------------------------ |
| Rev.1.0 | 2026-11-02 | xzyozi | 新規作成（Issueの記録）   |
