---
title: "課題記録: データ構造およびIngestion詳細設計"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "approved"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #4 の内容、背景、確定仕様、成果物をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/4"
snapshot_at: "2026-11-01"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
---

# 課題記録
**[DD/DS] データ構造仕様書(DS-001)およびIngestion詳細設計書(DD-001)の作成**

| 項目         | 内容                                                                     |
| :----------- | :----------------------------------------------------------------------- |
| Issue番号    | #4                                                                       |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/4                  |
| 状態         | Closed (PR #5 にてマージ完了)                            |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. Issue本文・概要

- **目的・背景**:
  - 基本設計書（`BD-001`）で合意されたアーキテクチャに基づき、確定しているコア仕様であるデータ構造（DS-001）およびIngestionエンジン（DD-001）の詳細設計書を作成する。
  - 壁打ち対象である「パーセンタイル計算の厳密な足切りラインや異常値判定ロジック」は実データ検証時に詰める方針とし、本フェーズでは確定しているBronze/Silverのスキーマおよびストリーミング処理契約を先行して固める。
- **対象範囲**:
  - GCS Filtered JSONL フォーマット定義
  - BigQuery Bronze層（Raw生ログ）およびSilver層（ファクト・ディメンション）スキーマ
  - GH Archive インフライト解凍・4イベント抽出・ゼロディスクI/Oストリーミング制御仕様
- **対象外**:
  - Gold層の微細な足切り閾値・パーセンタイル算出ロジックのチューニング（実データ投入後に決定）

## 2. 議論・決定・対応

- **決定事項**:
  - `DS-001_データ構造仕様書.md`: GCSおよびBigQuery Bronze/Silverの物理スキーマを定義。Gold層はカラム定義枠のみ示し、計算ロジックは実データ検証フェーズへ委譲。
  - `DD-001_Ingestionエンジン詳細設計書.md`: Python標準 `urllib.request` / `zlib` を活用したゼロディスクI/OストリーミングパイプラインとDTO、エラーハンドリングを定義。
- **対応状況**: 対応済み (PR #5 にて制定完了)
- **次のアクション**: なし（詳細設計制定完了）

## 3. 関連文書

- **基本設計書**: [`docs/design/BD-001_基本設計書.md`](../design/BD-001_基本設計書.md)
- **データ構造仕様書**: [`docs/design/DS-001_データ構造仕様書.md`](../design/DS-001_データ構造仕様書.md)
- **Ingestion詳細設計書**: [`docs/design/DD-001_Ingestionエンジン詳細設計書.md`](../design/DD-001_Ingestionエンジン詳細設計書.md)
