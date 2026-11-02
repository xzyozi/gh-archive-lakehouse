---
title: "ADR-002: リポジトリ作成の CreateEvent を抽出対象に追加する"
document_type: "architecture_decision_record"
document_number: "ADR-002"
document_role: "rationale"
ssot_scope: "decision_rationale"
status: "accepted"
version: "1.0"
created_at: "2026-11-02"
updated_at: "2026-11-02"
author: "xzyozi"
owner: "xzyozi"
purpose: "repo_age_days の導出のために、GH Archive の CreateEvent（リポジトリ作成のみ）を抽出対象へ追加し、Ingestion がBronze形式にフラット化して出力する判断と、その容量影響を記録するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "用語集（相対パス: ../../CONTEXT.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
  - "実データ検証報告書（相対パス: ../test/TEST-003_Bronzeフラット形式出力の実データ検証報告書.md）"
  - "課題記録（相対パス: ../features/ISSUE-014_Bronze取り込み方式とrepo_age導出.md）"
---

# ADR-002: リポジトリ作成の CreateEvent を抽出対象に追加する

| 項目           | 内容       |
| :------------- | :--------- |
| ステータス     | accepted   |
| 決定日         | 2026-11-02 |
| 決定者・承認者 | xzyozi     |
| 置換元ADR      | なし       |

## 1. 背景・課題

- **背景**: Gold層 `mart_oss_quadrant` の `repo_age_days` と `is_newborn_buzz`（開設7日以内かつ `daily_stars >= 10`）には、リポジトリの開設日が必要である。開設日は `dim_repositories.repo_created_at` に持つ設計だが、この値は GitHub API による Enrichment（上位200件のみ）に依存しており、対象外のリポジトリでは取得できなかった（Issue #14）。
- **背景（取り込み方式）**: 従来の GCS JSONL は GH Archive のネスト形式（`actor.login`, `repo.name`）のままで、Bronze `stg_github_events` のフラット形式（`actor_login`, `repo_name`, `event_date` 列あり）と一致しなかった。BigQuery のロードジョブだけでは取り込めない。
- **要求・制約**: 運用は無料枠内（GCS 5GB、BigQuery 10GB）に収める。`BD-001` 第4章は、イベント種別を追加する場合にADRを起票するよう定めている。
- **判断しない場合の影響**: Enrichment対象外のリポジトリで `repo_age_days` が得られず、新星OSSの検知が上位200件に限られる。Bronzeへの取り込みにも別の変換工程が必要になる。

## 2. 決定

1. GH Archive の `CreateEvent` のうち、`payload.ref_type == "repository"`（リポジトリ新規作成）のものだけを抽出対象に追加する。ブランチ・タグの `CreateEvent` は抽出しない。`TARGET_EVENT_TYPES`（4種）の集合は変更せず、判定関数 `is_target_event` で追加条件を扱う。
2. `repo_created_at` と `repo_age_days` は GitHub API を使わず、Bronze の `CreateEvent`（リポジトリ作成）の最小 `created_at` から導出する。Bronzeの保持期間（30日）より古いリポジトリは導出できず `NULL` とし、`is_newborn_buzz` は偽として扱う。
3. Ingestion が GH Archive の生イベントをメモリ上でフラット化し、`event_date` を付与して出力する。出力JSONLの列をBronzeと1対1に一致させ、BigQuery のロードジョブだけで取り込めるようにする。`id`, `type`, `actor_login`, `repo_name`, `created_at` のいずれかが欠けるイベントは出力せずスキップし、件数を `skipped_events` に集計する。

## 3. 理由・代替案

### 採用理由

1. 7日以内の新星を検知する目的では、7日分の `CreateEvent` がBronzeに必ず残っているため、30日より古いリポジトリが `NULL` になっても結果に影響しない。
2. API呼び出しが不要になり、GitHub APIのレート制限と認証情報の管理が増えない。
3. フラット化をIngestionで行うことで、ロードジョブ後の変換（SQL等）が不要になり、スキーマ契約が出力JSONL側で完結する。
4. リポジトリ作成のみに絞ることで、用途のないブランチ・タグ作成を出力に含めずに済む。

### 代替案

| 代替案                                           | 利点                           | 欠点・リスク                                                                         | 採用しなかった理由                                  |
| :----------------------------------------------- | :----------------------------- | :----------------------------------------------------------------------------------- | :-------------------------------------------------- |
| GitHub API で全対象リポジトリの開設日を取得      | 古いリポジトリも開設日が分かる | レート制限（認証なしで60回/時）に当たる。対象が数千件あり、現状の上位200件に限られる | 目的（7日以内の検知）に対して過大で、運用負荷が高い |
| `CreateEvent` を全種別（ブランチ・タグ含む）抽出 | 条件判定が単純                 | 件数が多く、無料枠を圧迫する                                                         | 用途がブランチ・タグにはない                        |
| ネスト形式のままGCSへ出力し、BigQuery側で変換    | Ingestionの変更が不要          | ロードジョブだけでは取り込めず、ステージング表と変換SQLが増える                      | 取り込みの工程と失敗箇所が増える                    |

## 4. 影響・トレードオフ

- **利点**: Enrichment対象外でも7日以内の新星を判定できる。ロードジョブだけでBronzeへ取り込める。
- **欠点・コスト**: 出力量が増える（下記）。Bronze保持期間（30日）より古いリポジトリの `repo_age_days` は `NULL` になる。REQUIRED欠落イベントは出力に含まれない。
- **容量への影響（実測）**: 2024-01-03 の 0時台・23時台（[`TEST-003`](../test/TEST-003_Bronzeフラット形式出力の実データ検証報告書.md)）で、`CreateEvent`（リポジトリ作成）を除いた場合 4.66MB / 4.55MB、含めた場合 5.74MB / 5.60MB。増分は 1.08MB / 1.04MB（約+23%）。`CreateEvent` は 4,083件 / 3,961件で、出力件数の約18%を占める。1日換算は約135MBで、GCSの7日保持は約1GB、Bronze（30日）のJSON換算は約4GBである。BigQuery内の実サイズは未計測のため、取り込み後に実測する。なお、従来のネスト形式（約6.2MB/時間、`2024-01-01-15`）より、フラット化後は `CreateEvent` を含めても小さい。
- **影響を受ける正本文書**: `DS-001`（第2.2節、第2.3節、第2.4節②、第2.6節）、`DD-001`（`project_event`、`IngestionSummaryDTO`）、`BD-001`（抽出イベントの記述）。
- **移行・ロールバック**: `CreateEvent` の抽出条件を外せば元の4種に戻せる。出力形式の変更は破壊的変更のため、既存のGCSオブジェクト（旧ネスト形式）は取り込めない。Bronzeへの取り込みは未運用のため、移行対象はない。
- **セキュリティ・運用上の影響**: 公開アクティビティのみを扱い、新たな認証情報は不要。

## 5. 関連文書・後続作業

- **要求・Issue**: [Issue #14](https://github.com/xzyozi/gh-archive-lakehouse/issues/14)、[`ISSUE-014`](../features/ISSUE-014_Bronze取り込み方式とrepo_age導出.md)
- **現在の仕様**: [`DS-001`](../design/DS-001_データ構造仕様書.md)、[`DD-001`](../design/DD-001_Ingestionエンジン詳細設計書.md)、[`BD-001`](../design/BD-001_基本設計書.md)
- **実装・検証タスク**: `octopulse.ingestion.stream_engine.is_target_event`、`octopulse.ingestion.gcs_uploader.project_event`、[`TEST-003`](../test/TEST-003_Bronzeフラット形式出力の実データ検証報告書.md)
- **後続ADR**: なし

## 6. 例外・見直し条件

- **例外・適用差異**: なし
- **見直し条件**: Bronzeの保持期間を30日から短縮する場合、または `is_newborn_buzz` の「7日以内」を延長する場合は、導出可能な範囲が変わるため再評価する。BigQuery内の実サイズが無料枠を圧迫した場合も再評価する。
- **見直し時期**: Bronzeへの取り込みを開始した後

## 7. 作成完了チェック

- [x] この判断がADRを必要とする重要度か確認した。
- [x] 背景、決定、理由、代替案、影響を記入した。
- [x] 現在の仕様を定義する正本文書へのリンクを設定した。
- [x] 判断変更時は本書を削除せず、後継ADRを作成する方針を確認した。
- [x] 機密情報や認証情報を記載していない。

## 8. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)           |
| :------ | :--------- | :----- | :--------------------------------- |
| Rev.1.0 | 2026-11-02 | xzyozi | 新規作成（Issue #14 の決定を記録） |
