---
title: "課題記録: Bronze取り込み方式とrepo_age導出"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
version: "1.0"
created_at: "2026-11-02"
updated_at: "2026-11-02"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #14 の Bronze取り込み方式と repo_age_days 導出方法の決定内容、および対応状況をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/14"
snapshot_at: "2026-11-02"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
  - "ADR-002（相対パス: ../adr/002-ingest-repository-create-event.md）"
  - "実データ検証報告書（相対パス: ../test/TEST-003_Bronzeフラット形式出力の実データ検証報告書.md）"
---

# 課題記録
**Bronze取り込み方式と repo_age_days の導出**

> Issueの内容をリポジトリ内へ残すための簡易記録です。Issue自体が最新情報の正本です。

| 項目         | 内容                                                     |
| :----------- | :------------------------------------------------------- |
| Issue番号    | #14                                                      |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/14 |
| 状態         | 実装中（PR作成待ち）                                     |
| 作成者       | xzyozi                                                   |
| 担当者       | xzyozi                                                   |
| 記録日時     | 2026-11-02                                               |
| 最終確認日時 | 2026-11-02                                               |

## 1. Issue本文・概要

- **目的・背景**:
  - GCS の JSONL が GH Archive のネスト形式（`actor.login`, `repo.name`）のままで、BigQuery の Bronze `stg_github_events`（フラット形式、`event_date` 列あり）と一致せず、ロードジョブだけでは取り込めない。
  - `is_newborn_buzz` に必要な `repo_age_days` が、GitHub API による Enrichment（上位200件）に依存しており、対象外のリポジトリでは取得できない。
- **対象範囲**: Ingestion の出力形式（Bronze との一致）、抽出対象イベントの追加、`repo_created_at` / `repo_age_days` の導出方針、関連ドキュメントの更新。
- **対象外**: BigQuery へのロードジョブ実装、dbt モデル、`repo_age_days` を導出するSQLの実装。

## 2. 議論・決定・対応

- **主な論点**: フラット化の実施場所（Ingestion かBigQuery側か）、開設日の取得手段（GitHub API か GH Archive のイベントか）。
- **決定事項**:
  1. 取り込み方式: Ingestion側（メモリ上）でフラット化し、`event_date` を付与して出力する。出力JSONLの列をBronzeと完全に一致させ、BigQueryのロードジョブだけで取り込めるようにする。
  2. `repo_age_days`: GitHub API は使わず、GH Archive の `CreateEvent`（`payload.ref_type == "repository"` のみ）から開設日を導出する。ブランチ・タグの `CreateEvent` は抽出しない。Bronze保持期間（30日）より古いリポジトリは `NULL` とし、`is_newborn_buzz` は偽として扱う。
  3. REQUIRED列が欠けたイベントは出力せずスキップし、`skipped_events` に集計する。
- **対応状況**: 対応中。実装、ユニットテスト、実データ検証（`TEST-003`）、ドキュメント更新は完了し、PR作成待ち。
- **次のアクション**: PRを作成してレビューを受ける。マージ後、Bronzeへのロードジョブと `dim_repositories.repo_created_at` の導出SQLを別Issueで実装し、BigQuery内の実サイズを実測する。

## 3. 関連文書

- **要求・機能仕様**: [`DS-001`](../design/DS-001_データ構造仕様書.md) 第2.2節・第2.3節・第2.4節②・第2.6節
- **設計書・ADR**: [`DD-001`](../design/DD-001_Ingestionエンジン詳細設計書.md)、[`BD-001`](../design/BD-001_基本設計書.md)、[`ADR-002`](../adr/002-ingest-repository-create-event.md)
- **実装・レビュー・テスト**: `src/octopulse/ingestion/stream_engine.py`（`is_target_event`）、`src/octopulse/ingestion/gcs_uploader.py`（`project_event`）、[`TEST-003`](../test/TEST-003_Bronzeフラット形式出力の実データ検証報告書.md)

## 4. 分析・知見（必要な場合のみ）

- **確認した事実**: `2024-01-03` の0時台・23時台で、`CreateEvent`（リポジトリ作成）は約4,000件/時間、出力量の増分は約1.0〜1.1MB/時間（約+23%）。フラット化後の合計は約5.6〜5.7MB/時間で、従来の約6.2MB/時間より小さい。`event_date` がファイル名の日付とずれるイベントは0件。
- **再利用できる知見**: 1件でもREQUIRED列が欠けた行があるとBigQueryのロードジョブ全体が失敗するため、出力前に除外する。
- **昇格先**: [`ADR-002`](../adr/002-ingest-repository-create-event.md)

## 5. 記録上の注意

- [x] Issue URLと取得日時を記録した。
- [x] Issueの写しと現在の正本文書を混同していない。
- [x] 決定事項がある場合は、関連する正本文書またはADRへのリンクを設定した。
- [x] 機密情報、認証情報、利用者固有の情報を転記していない。

## 6. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)            |
| :------ | :--------- | :----- | :---------------------------------- |
| Rev.1.0 | 2026-11-02 | xzyozi | 新規作成（Issue #14 の記録）        |
