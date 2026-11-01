---
title: "課題記録: CHAOSSモデリング検討"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "approved"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "Issue #6 の壁打ち論点（計算式・足切り閾値）をリポジトリ内に記録・追跡するため"
source_issue: "https://github.com/xzyozi/gh-archive-lakehouse/issues/6"
snapshot_at: "2026-11-01"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "実データ検証報告書（相対パス: ../test/TEST-001_Ingestionストリーミング実データ検証報告書.md）"
---

# 課題記録
**[Modeling] CHAOSS 2軸評価モデル（計算ロジック・足切り閾値）の策定**

| 項目         | 内容                                                                     |
| :----------- | :----------------------------------------------------------------------- |
| Issue番号    | #6                                                                       |
| Issue URL    | https://github.com/xzyozi/gh-archive-lakehouse/issues/6                  |
| 状態         | Closed (策定・合意完了)                                                  |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. 検討論点と検証データからの示唆

1. **母集団の足切りライン**:
   - `TEST-001` の結果より、1時間で活動のあったユニークリポジトリ約7,000件のうち、大半が単発1件のイベント。
   - 日次換算で「WatchEvent >= 3 かつ (Fork+Issue+PR) >= 1」を設けることで、約1,000〜3,000件の有望リポジトリ群に絞り込めると判明。
2. **Attention Velocity (X軸)**:
   - 日次新規Star獲得数のパーセンタイル順位（`PERCENT_RANK() OVER (PARTITION BY target_date ORDER BY daily_stars ASC)`）。
3. **Engagement Depth (Y軸)**:
   - PRがForkの約7倍発生するため、発生頻度を考慮した加重複合スコア `(daily_forks * 3) + (daily_prs * 2) + (daily_issues * 1)` のパーセンタイル順位を採用。
4. **4象限セグメンテーション（Cutoff）**:
   - 上位20%（パーセンタイル >= 0.80）を境界値とし、Hyper-Growth, Viral Spotlight, Silent Utility, Niche に分類。
5. **スパム・ボット検知フラグ**:
   - `is_suspicious_star_surge`: `daily_stars >= 10 AND (daily_forks + daily_prs) = 0`
   - `is_bot_generated_prs`: `daily_prs >= 50 AND daily_stars = 0`（`mergifyio-testing` 等のテスト自動化ボット除外用）
   - `is_newborn_buzz`: `repo_age_days <= 7 AND daily_stars >= 10`

## 2. 決定事項および対応状況

- **決定事項**: 上記の確定仕様を `DS-001_データ構造仕様書.md` 第2.6節へ正式反映。
- **対応状況**: 対応済み（DS-001 Rev.1.1 反映完了）
- **次のアクション**: dbt Silver/Gold モデル実装時に上記ロジックをSQL化する。
