---
title: "Ingestionストリーミング実データ検証報告書"
document_type: "test_plan_report"
document_number: "OCTO-TEST-001"
document_role: "evidence"
ssot_scope: "test_evidence"
status: "approved"
version: "1.0"
created_at: "2026-11-01"
updated_at: "2026-11-01"
author: "xzyozi"
owner: "xzyozi"
purpose: "GH Archiveからのインフライトストリーミング解凍・4イベント抽出（ゼロディスクI/O）の実データ検証結果を記録し、モデリング検討（Issue #6）の判断材料とするため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "Ingestion詳細設計書（相対パス: ../design/DD-001_Ingestionエンジン詳細設計書.md）"
  - "データ構造仕様書（相対パス: ../design/DS-001_データ構造仕様書.md）"
  - "課題記録 Issue #7（相対パス: ../features/ISSUE-007_Ingestion実データ抽出検証.md）"
---

# テスト計画・結果報告書
**OCTO-TEST-001: Ingestionストリーミング実データ抽出・性能検証報告書**

---

## 1. 目的・対象範囲

- **目的**:
  - `DD-001` で規定された「インフライトストリーミング解凍」および「ゼロディスクI/O」が実データ（GH Archive実ファイル約83MB）で正常稼働することを実証する。
  - メモリ消費量が非機能目標（512MB以下）を大幅に下回ることを確認する。
  - 抽出された実イベントの分布・比率を確認し、CHAOSSモデリング（Issue #6: 足切りライン・重み付け）の客観的根拠を得る。
- **テスト対象**:
  - `src/octopulse/ingestion/stream_engine.py`
  - `scripts/verify_ingestion_stream.py`
- **対象データ**:
  - GH Archive 実アーカイブ: `2024-01-01-15.json.gz`（実圧縮サイズ 約83.1MB）

---

## 2. テスト結果サマリー

| 指標 | 目標値・基準 | 実測値 | 判定 |
| :--- | :--- | :--- | :---: |
| **ローカルディスク書き込み量** | 0 bytes（ゼロディスクI/O） | **0 bytes** | **合格** |
| **ピークメモリ消費量** | 512 MB 以下 | **4.48 MB** | **合格** |
| **処理所要時間 (1時間分)** | 60 秒以下 | **6.53 秒** | **合格** |
| **対象4イベント抽出件数** | 正常パース・抽出 | **23,911 件** | **合格** |
| **単体テスト通過率** | 100% | **2 / 2 通過 (100%)** | **合格** |

---

## 3. 実データ分析結果（抽出内訳 ＆ 分布）

### 3.1 対象4イベントの内訳

| イベント種別 | 抽出件数 | 構成比 | 考察 |
| :--- | :---: | :---: | :--- |
| `PullRequestEvent` | 12,470 件 | 52.2% | GitHub全体で最もトランザクション量が多い開発アクティビティ。 |
| `WatchEvent`（Star） | 7,236 件 | 30.3% | 注目のモメンタム（Attention Velocity）を測定するコアシグナル。 |
| `IssuesEvent` | 2,443 件 | 10.2% | バグ報告やディスカッションなど利用現場のリアルな反応。 |
| `ForkEvent` | 1,762 件 | 7.4% | ローカル環境への複製・検証を示すEngagementシグナル。 |

- **アクティブなユニークリポジトリ数**: **7,041 リポジトリ**

---

### 3.2 抽出された上位リポジトリのサンプル

#### ① Star（WatchEvent）獲得数上位リポジトリ（1時間あたり）
1. `movie-web/movie-web`: +111 stars（Forks: 10, PRs: 0）→ **典型的な Viral Spotlight（SNSバズ先行）**
2. `practical-tutorials/project-based-learning`: +33 stars
3. `KRTirtho/spotube`: +24 stars
4. `janhq/jan`: +24 stars（LLM新星OSS）
5. `mlabonne/llm-course`: +23 stars
6. `tokenami/tokenami`: +22 stars
7. `dvmazur/mixtral-offloading`: +21 stars（Mixtral新星OSS）

#### ② PR（PullRequestEvent）活動数上位リポジトリ（1時間あたり）
1. `mergifyio-testing/functional-testing-repo-Greesb`: 310 PRs（Stars: 0）→ **テスト自動化ボット**
2. `WolseyBankWitness/rediffusion`: 90 PRs
3. `starknet-io/starknet-website`: 77 PRs（Stars: 1）

---

## 4. Issue #6（CHAOSSモデリング壁打ち）への示唆・提言

実データの計測結果から、以下の極めて重要な知見が得られた：

1. **足切りライン（母集団）の妥当性**:
   - 1時間でStarがついたリポジトリは約2,000件程度だが、その大半は「+1 star」の散発的イベント。
   - 当初仮案の「WatchEvent >= 3 かつ (Fork+Issue+PR) >= 1」を日次（24時間累計）に適用すると、ノイズリポジトリが90%以上カットされ、有意義な約1,000〜3,000件の上位アクティブ群に絞り込めることが確認できた。
2. **スパム・ボット検知フラグの必須性**:
   - PR上位に現れた `mergifyio-testing` のように、CI/CDテストボットによる大量PRのみで構成されるリポジトリが実際に存在する。
   - Silver/Gold層における異常値フラグ（StarとEngagementの乖離度）の導入は、分析精度担保のために不可欠であることが実証された。
3. **Engagement Depth の加重設計**:
   - PR数はFork数の約7倍発生しているため、単純合算ではなく、イベントごとの発生頻度を考慮した加重比率（例: `Fork * 2 + Issue * 1 + PR * 1`、または各指標の個別パーセンタイル合算）が望ましい。

---

## 5. 改訂履歴 (Change Log)

| 版数 | 改訂日 | 変更者 | 変更内容・変更理由 (Why) |
| :--- | :--- | :--- | :--- |
| Rev.1.0 | 2026-11-01 | xzyozi | 新規作成（初版制定: Issue #7 検証結果報告） |
