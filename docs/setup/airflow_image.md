---
title: "環境構築手順書: Airflow 自作イメージ（Cosmos + dbt-bigquery）"
document_type: "environment_setup_procedure"
document_role: "procedure"
ssot_scope: "environment_setup"
status: "draft"
version: "1.0"
created_at: "2026-11-02"
updated_at: "2026-11-02"
author: "xzyozi"
owner: "xzyozi"
purpose: "dbt-bigquery と astronomer-cosmos を含む Airflow 自作イメージの構成、バージョンピン、ビルド・更新手順を記録するため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "基本設計書（相対パス: ../design/BD-001_基本設計書.md）"
  - "課題記録 Issue #16（相対パス: ../features/ISSUE-016_リージョン決定とAirflowイメージ.md）"
---

# 環境構築手順書
**Airflow 自作イメージ（Cosmos + dbt-bigquery）**

---

## 1. 目的・対象範囲

- **目的**: `BD-001` のアーキテクチャ（Astronomer Cosmos 経由の dbt 実行）を満たす Airflow イメージを構築・更新する。
- **対象環境**: ローカル開発（Docker Compose。Windows / WSL2 / Linux）。
- **対象者**: 開発者。
- **対象外**: dbt プロジェクト・DAG の実装、本番環境へのデプロイ。

## 2. 前提条件・注意事項

- **参照する設計**: `BD-001`（3.2節）。
- **必要なツール**: Docker Engine（Docker Desktop + WSL2 バックエンド可）と Docker Compose v2。
- **必要な権限**: ホストで `gcloud auth application-default login` 済みの ADC。認証情報はリポジトリへ置かず、読み取り専用でマウントする。
- **安全上の注意**: `airflow/.env` は Git 管理外。`.dockerignore` により `.env` と `logs/` はイメージに入らない。
- **中断条件**: ビルドで依存解決に失敗した場合は、ピンを変更せず原因（constraints との衝突）を記録して停止する。

### 2.1 イメージ構成

| 環境 | 場所 | 導入物 | 目的 |
| :--- | :--- | :--- | :--- |
| Airflow 本体 | システム site-packages | `astronomer-cosmos` | dbt プロジェクトを Airflow タスクとして描画・実行 |
| dbt 専用 venv | `/opt/airflow/dbt_venv` | `dbt-core`, `dbt-bigquery` | dbt 実行。Airflow 本体と依存を分離 |

**dbt_venv を分ける理由**: dbt と Airflow は共通ライブラリ（protobuf、google-cloud 系など）のバージョン要求が衝突しやすい。Cosmos の公式推奨に従い dbt を別 venv に隔離する。Cosmos 側は `ExecutionMode.LOCAL` と `ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt")` で venv の dbt を指す。

イメージは公式の非 root ユーザー `airflow` のまま構築し、`pip install` は `--no-cache-dir` でキャッシュを残さない。

### 2.2 バージョンピンと選定根拠

サプライチェーン対策として、PyPI 公開から3日以上経過した版のみを採用した（確認日 2026-11-02）。

| パッケージ | 版 | PyPI 公開日 (UTC) | 選定根拠 |
| :--- | :--- | :--- | :--- |
| astronomer-cosmos | 1.15.1 | 2026-08-04 | `requires-python>=3.10`、`apache-airflow>=2.9.0` のため Airflow 2.9.1 / Python 3.11 で使用可能。公開から約3か月経過 |
| dbt-bigquery | 1.11.3 | 2026-06-24 | `dbt-core>=1.11.6,<2.0` の 1.x 系。`requires-python>=3.10` |
| dbt-core | 1.11.16 | 2026-10-14 | dbt-bigquery 1.11.3 の要求を満たす 1.11 系の最新版。公開から19日経過 |

- Airflow 本体の側は、公式 constraints（`constraints-2.9.1/constraints-3.11.txt`）を `pip install --constraint` に指定し、Airflow の依存を壊さない。
- dbt_venv は Airflow と独立のため constraints を使わない。再現性は dbt-core と dbt-bigquery の完全ピンで担保する（推移的依存は未固定）。

## 3. 構築手順

| No. | 操作・設定 | 期待結果 | 異常時の対応 |
| :-- | :--------- | :------- | :----------- |
| 1 | `airflow/.env.example` を `airflow/.env` にコピーし、`_AIRFLOW_WWW_USER_PASSWORD`、`GCP_PROJECT_ID`、必要なら `GCLOUD_CONFIG_DIR` を設定する | 必須変数が揃う | 未設定だと compose が `Set _AIRFLOW_WWW_USER_PASSWORD` で停止する。値を設定する |
| 2 | `docker compose -f airflow/docker-compose.yaml build` | `octopulse-airflow:local` が作成される | pip の依存解決エラーは全文を記録して停止（2章の中断条件） |
| 3 | `docker compose -f airflow/docker-compose.yaml up airflow-init` | DB 初期化と管理ユーザー作成が完了 | ログで DB 接続を確認 |
| 4 | `docker compose -f airflow/docker-compose.yaml up -d` | Web UI が `http://127.0.0.1:8080` で開く | `docker compose ... logs airflow-webserver` を確認 |

### 3.1 ADC マウントと Windows / WSL2 の注意

- ホストの ADC ディレクトリを `/home/airflow/.config/gcloud` に読み取り専用でマウントする。固定の JSON キーは使わない。
- 既定は `~/.config/gcloud`。Windows の Docker Desktop では ADC が `%APPDATA%\gcloud` にあるため、`airflow/.env` の `GCLOUD_CONFIG_DIR` に絶対パスを指定する。WSL2 内で実行する場合は WSL 側で `gcloud auth application-default login` を行い、既定パスを使う。
- Web UI は `127.0.0.1` のみに公開している。

## 4. 構築後の確認

| 確認項目 | 確認方法 | 合格条件 |
| :------- | :------- | :------- |
| Cosmos の導入 | `docker compose -f airflow/docker-compose.yaml run --rm airflow-init python -c "import cosmos; print(cosmos.__version__)"` | `1.15.1` が表示される |
| dbt の導入 | `docker compose -f airflow/docker-compose.yaml run --rm airflow-init /opt/airflow/dbt_venv/bin/dbt --version` | dbt-core 1.11.16 と bigquery 1.11.3 が表示される |
| 非 root 実行 | `docker compose -f airflow/docker-compose.yaml run --rm airflow-init id -u` | `50000`（または `AIRFLOW_UID`）が表示される |

注: 上記の確認コマンドは未実施。ビルド後に実施すること（`airflow-init` は `command: version` のため、確認時はコマンド引数で上書きされる前提。entrypoint の挙動により動かない場合は `--entrypoint` を指定する）。

## 5. 更新手順（依存更新）

1. PyPI JSON API で候補版の公開日を確認する。`https://pypi.org/pypi/<package>/json` の `releases["<版>"][].upload_time_iso_8601` が**現在から3日以上前**であること。
2. `requires_python` と `requires_dist` を確認する。Cosmos は `apache-airflow` の下限、dbt-bigquery は `dbt-core` の範囲を満たすこと。
3. `airflow/Dockerfile` のピンと冒頭コメントの表、本書 2.2 節を同時に更新する。
4. `docker compose -f airflow/docker-compose.yaml build --no-cache` で再ビルドし、4章の確認を行う。
5. Airflow 本体の版を上げる場合は、`AIRFLOW_VERSION` と `FROM` のタグ、constraints URL を揃えて更新する。

## 6. 関連文書

- **設計書**: [BD-001 基本設計書](../design/BD-001_基本設計書.md)
- **Issue記録**: [ISSUE-016](../features/ISSUE-016_リージョン決定とAirflowイメージ.md)

## 7. 作成完了チェック

- [x] `document_role`、`ssot_scope`、`status`、`owner`をFrontmatterへ設定した。
- [x] 目的、対象環境、前提条件を記載した。
- [x] 各手順に操作内容、期待結果、異常時の対応を記載した。
- [x] 構築後の確認方法と合格条件を記載した（実施は未）。
- [x] 秘密情報や利用者固有の認証情報を記載していない。

## 8. 改訂履歴 (Change Log)

| 版数    | 改訂日     | 変更者 | 変更内容・変更理由 (Why)                        |
| :------ | :--------- | :----- | :---------------------------------------------- |
| Rev.1.0 | 2026-11-02 | xzyozi | 新規作成（Airflow 自作イメージの手順制定: Issue #16） |
