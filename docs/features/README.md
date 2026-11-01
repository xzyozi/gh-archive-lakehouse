---
title: "docs/features 運用方針"
document_type: "directory_guide"
document_role: "procedure"
ssot_scope: "features_directory_operation"
status: "draft"
version: "1.0"
created_at: "2026-10-23"
updated_at: "2026-10-23"
author: "[作成者 / チーム名]"
owner: "[責任者 / チーム名]"
purpose: "要求・機能仕様の管理先をIssueと docs/features/ で使い分けるフォールバック方針を示すため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "ADR-001 文書ガバナンスとSSOT（相対パス: ../adr/001-document-governance-and-ssot.md）"
  - "課題記録テンプレート（相対パス: ./TEMPLATE/TEMPLATE_課題記録.md）"
  - "Git Flow 運用規約（相対パス: ../setup/git_flow_and_branch_policy.md）"
---

# docs/features 運用方針

`docs/features/` は、要求・機能仕様の**フォールバック先**として運用する。Issueで管理できるものはIssueを正とし、Issueで管理できないものだけをここに置く。

## 1. 基本方針

| 区分                               | 管理先                                 | 判断基準                  |
| :--------------------------------- | :------------------------------------- | :------------------------ |
| Git管理下 **かつ** Issue操作が可能 | **GitHub Issue（第一選択）**           | 次の2条件を両方満たす     |
| 上記以外                           | **`docs/features/`（フォールバック）** | いずれか1つでも満たさない |

### Issue管理の適用条件（両方を満たすこと）

1. 対象がGit管理下にあり、GitHubのリポジトリ（リモート）に紐づいている。
2. `gh` などでIssueの参照・起票ができる（インストール済み、認証済み、対象リポジトリへの権限あり）。

### 判断の流れ

1. 対象がGit管理下か。管理外なら `docs/features/` で管理する。
2. `gh` などでIssueを操作できるか。できなければ `docs/features/` で管理する。
3. 両方を満たす場合のみ、Issueで管理する。

次のような場合も、フォールバックとして `docs/features/` を使う。

- `gh` が未導入、未認証、または権限不足で、すぐに解消できない。
- ネットワーク制限などでGitHubに接続できない。
- Git管理外の成果物や業務である。

後から条件を満たせるようになった場合は、`docs/features/` の内容をIssueへ移し、文書の `status` を `superseded` にして移行先のIssueを記録する。

## 2. Issueで管理する場合（Git管理下かつ `gh` 等が使える）

- 要求、背景、受け入れ条件、議論、決定はIssueに書く。`docs/features/` に重複して書かない。
- ブランチは `feat/<機能名>` などの規約に従い、コミットは `[feat] 概要` の形式にする（[Git Flow 運用規約](../setup/git_flow_and_branch_policy.md)）。
- 次の場合に限り、Issueの内容を `docs/features/` に写してよい。
  - Issueが使えなくなる場合に備え、リポジトリ内に履歴を残したい。
  - 他の文書（設計書、テスト報告書）から、リポジトリ内の相対パスで参照したい。
  - 監査などで、取得時点の内容を固定したい。
- 写す場合は [課題記録テンプレート](./TEMPLATE/TEMPLATE_課題記録.md) を使い、`source_issue` と `snapshot_at` を必ず設定する。写しは履歴であり、最新の正本はIssue側とする。

### 現在の仕様はIssueに置かない

[ADR-001](../adr/001-document-governance-and-ssot.md) の方針に従い、Issueコメントは議論の履歴として扱う。仕様として確定した内容は、次の正本へ反映する。

| 確定した内容                  | 反映先                 |
| :---------------------------- | :--------------------- |
| アーキテクチャ・責務・境界    | 基本設計書（BD）       |
| モジュール制御・関数契約・DTO | 詳細設計書（DD）       |
| 永続化スキーマ                | データ構造仕様書（DS） |
| 外部API・メッセージ           | IF/API仕様書           |
| 判断理由・代替案              | ADR                    |

Issueには、反映先へのリンクを残す。

## 3. `docs/features/` で管理する場合（Git管理外、または `gh` 等が使えない）

既存のやり方に従い、この配下に文書を置く。

1. `TEMPLATE/TEMPLATE_課題記録.md` を `docs/features/` 直下にコピーする（`TEMPLATE/` には実文書を置かない）。
2. ファイル名は軽量文書の規約に従う。例は `認証機能の要望.md` や `ISSUE-007_ログイン仕様の確認.md` である。
3. Frontmatterを埋める。`source_issue` は、Issue番号がなければ管理元の識別子（依頼票番号など）を入れるか、「なし」とする。
4. `related_documents` の相対パスは、コピー先に合わせて1階層浅くする（`../../README.md` → `../README.md`）。
5. 状態は `draft` → `review` → `approved` と進める。不要になった文書は削除せず、`superseded` または `deprecated` にする。
6. 決定事項は、2章の表の反映先（BD/DD/DS/IF/ADR）へ反映し、リンクを残す。

## 4. 命名と置き場所の早見表

| ケース                                        | 置き場所                               | 例                                |
| :-------------------------------------------- | :------------------------------------- | :-------------------------------- |
| Git管理下かつ `gh` 等が使える新機能           | Issue                                  | `#123 ログイン機能の追加`         |
| 上記でIssueを写す                             | `docs/features/ISSUE-<番号>_<題名>.md` | `ISSUE-123_ログイン機能の追加.md` |
| Git管理外、または `gh` 等が使えない要望・要求 | `docs/features/<題名>.md`              | `帳票出力の要望.md`               |

## 5. 注意事項

- 同じ要求をIssueと `docs/features/` の両方で更新し続けない。正本を1つに決める。
- 機密情報、認証情報、利用者固有の情報を転記しない。
- `REQ`（正式な要求・機能仕様書）のテンプレートは未整備である。必要になった時点で、ガバナンスの規約に従い `TEMPLATE/` に追加する。
