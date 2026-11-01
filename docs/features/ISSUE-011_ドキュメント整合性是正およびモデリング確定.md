---
title: "課題記録: ドキュメント整合性是正およびモデリング確定"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #11 のドキュメント・実装間の整合性是正およびCHAOSSモデリング確定内容をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/11"
snapshot_at: "2026-11-01"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
---

# 課題記録
**[Governance] ドキュメント・実装間の整合性是正およびCHAOSSモデリング確定**

| 項目         | 内容                                                                     |
| :----------- | :----------------------------------------------------------------------- |
| Issue番号    | #11                                                                      |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/11                 |
| 状態         | Open                                                                     |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. Issue本文・概要

- **目的**:
  - 推奨される5つの是正ステップに基づき、基本設計書（BD-001）、データ構造仕様書（DS-001）、詳細設計書（DD-001）、課題記録、および実装コード間の整合性を是正する。
  - 実データ検証（`TEST-001`）を踏まえたモデリング計算式（Issue #6）を `DS-001` へ正式反映する。

## 2. 是正内容サマリー

1. **DS-001 ER図の修正**:
   - `payload: JSON`, `topics: ARRAY<STRING>`, `event_action` 追加など、テーブル定義表と完全一致させた。
2. **DD-001 関数の同期**:
   - `run_daily_ingestion` の引数（`storage_client`, `hours`）および `run_daily_ingestion_to_writer` 契約を追加・同期。
3. **BD-001 参照の更新**:
   - 2.2節のコンポーネントマッピングの `(予定)` 表記を実在確定リンク（`DD-001`, `DS-001`）へ更新。
4. **docs/features/ 課題記録の同期**:
   - `ISSUE-002` および `ISSUE-004` の状態を Closed へ更新し、Frontmatter を `approved` に進めた。
5. **Issue #6 の壁打ち反映**:
   - 足切り基準（Watch>=3 かつ アクティビティ>=1）、Attention Velocity（Starパーセンタイル）、Engagement Depth（加重Fork*3+PR*2+Issue*1のパーセンタイル）、4象限Cutoff（Top 20% = 0.80）、およびボット・スパム検知フラグを `DS-001` 第2.6節に確定反映。

## 3. 関連文書

- **基本設計書**: [`docs/design/BD-001_基本設計書.md`](../design/BD-001_基本設計書.md)
- **データ構造仕様書**: [`docs/design/DS-001_データ構造仕様書.md`](../design/DS-001_データ構造仕様書.md)
- **Ingestion詳細設計書**: [`docs/design/DD-001_Ingestionエンジン詳細設計書.md`](../design/DD-001_Ingestionエンジン詳細設計書.md)
