---
title: "課題記録: CHAOSSモデリング検討"
document_type: "issue_record"
document_role: "reference"
ssot_scope: "issue_record"
status: "draft"
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
| 状態         | Open (壁打ちプール中)                                                    |
| 作成者       | xzyozi                                                                   |
| 担当者       | xzyozi                                                                   |
| 記録日時     | 2026-11-01                                                               |
| 最終確認日時 | 2026-11-01                                                               |

## 1. 検討論点と検証データからの示唆

1. **母集団の足切りライン**:
   - `TEST-001` の結果より、1時間で活動のあったユニークリポジトリ約7,000件のうち、大半が単発1件のイベント。
   - 日次換算で「WatchEvent >= 3 かつ (Fork+Issue+PR) >= 1」を設けることで、約1,000〜3,000件の有望リポジトリ群に絞り込めると判明。
2. **Attention Velocity (X軸)**:
   - 日次新規Star獲得数のパーセンタイル、または直近7日移動平均。
3. **Engagement Depth (Y軸)**:
   - PRがForkの7倍発生するため、発生頻度を考慮した加重（Fork * 2 + Issue * 1 + PR * 1 等）が有効。
4. **スパム・ボット検知フラグ**:
   - テスト自動化ボット（PR 300件超・Star 0件）を排除するためのフラグ設計が必要。

## 2. 次のアクション

- 本検証結果を元に、ユーザーとの最終壁打ちを行い、dbtモデル実装時にロジックを確定する。
