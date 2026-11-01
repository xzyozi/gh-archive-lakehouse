---
title: "課題記録: OctoPulse基本設計書の作成"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #2 の内容、背景、検討論点、決定事項をリポジトリ内に記録するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/2"
snapshot_at: "2026-11-01"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "用語集（相対パス: ../../CONTEXT.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
---

# 課題記録
**[BD] OctoPulse 基本設計書の作成（システムアーキテクチャ・CHAOSS評価モデル策定）**

| 項目         | 内容                                                                     |
| :----------- | :----------------------------------------------------------------------- |
| Issue番号    | #2                                                                       |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/2                  |
| 状態         | Open                                                                     |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. Issue本文・概要

- **目的・背景**:
  - GH Archiveを活用し、「突如バズを起こした急浮上リポジトリ（未知の新星OSS）」を早期検知・発掘するデータレイクハウス「OctoPulse」の基本設計書を作成する。
  - 単なるStar集計ではなく、CHAOSS思想に準拠した「Attention Velocity（注目の拡散速度）」×「Engagement Depth（実利用・開発の定着深度）」の2軸パーセンタイル評価モデルを定義する。
  - WIF認証、インフライトストリーミング、スタースキーマモデリング、完全0円防衛（Always Free枠内）のエンジニアリング規律を確立する。
- **対象範囲**:
  - システム全体アーキテクチャおよびデータフロー（Ingestion, Storage, DWH, BI）
  - データパイプラインおよびゼロディスクI/Oストリーミング仕様
  - メダリオン構成（Bronze/Silver/Gold）およびCHAOSS 2軸モデリング仕様
  - セキュリティ（WIF, ADCマウント）およびコスト防衛ライン仕様
- **対象外**:
  - 個別関数の具象実装、詳細クエリ・テーブルDDL定義（詳細設計書・データ構造仕様書で定義）

## 2. 議論・決定・対応

- **主な論点と決定事項**:
  1. **GH Archive 24時間分処理方式**:
     - *決定*: 日次バッチ（Daily）で前日24時間分のファイルを逐次ストリーム処理し、1つの日別JSONLに集約する方式を採用。ローカルDockerの常時起動を回避し、Backfillの運用性を最大化する。
  2. **GitHub API Enrichment**:
     - *決定*: 前日比Star急伸上位200件を対象とし、GraphQL APIによりバッチ取得。既存マスタに存在しないリポジトリのみを差分補完することで、5,000 req/hの制限を確実に温存する。
  3. **Airflow × dbt統合**:
     - *決定*: `astronomer-cosmos` を採用。dbtの各レイヤー（Staging/Intermediate/Mart）およびテストをAirflowのネイティブタスクツリーとして可視化・制御する。
  4. **パーセンタイル計算の母集団**:
     - *決定*: 「WatchEvent >= 3 かつ (Fork + Issue + PR) >= 1」を足切りスクリーニング基準とし、休眠・単発リポジトリによる分布の歪みを排除する。
  5. **ボット・スパム対策**:
     - *決定*: Silver/Gold層に `is_suspicious_star_surge` および `is_newborn_buzz` の異常値フラグを定義し、BI側で除外・追跡可能とする。
  6. **4象限セグメンテーション境界値**:
     - *決定*: パレートの法則に基づき、上位20%（パーセンタイル >= 0.80）を「High」と定義する。
  7. **Looker Studio課金回避**:
     - *決定*: 抽出データソース（1日1回更新）を必須とし、第三者閲覧によるBigQueryスキャンを完全0化する。
  8. **Terraformステート管理**:
     - *決定*: GCSバックエンド（US Region）を採用し、完全0円かつCI/CD対応可能な構成とする。
- **対応状況**: 対応中
- **次のアクション**: 基本設計書 `docs/design/BD-001_基本設計書.md` の作成およびPR提出。

## 3. 関連文書

- **基本設計書**: [`docs/design/BD-001_基本設計書.md`](../design/BD-001_基本設計書.md)
- **Git Flow 運用規約**: [`docs/setup/git_flow_and_branch_policy.md`](../setup/git_flow_and_branch_policy.md)
- **ドキュメントガバナンス**: [`docs/README.md`](../README.md)
