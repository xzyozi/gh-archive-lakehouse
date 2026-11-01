---
title: "ドキュメント検証スクリプト運用"
document_type: "environment_setup_procedure"
document_role: "procedure"
ssot_scope: "environment_setup"
status: "draft"
version: "1.0"
created_at: "2026-10-28"
updated_at: "2026-10-28"
author: "プロジェクトチーム"
owner: "プロジェクトチーム"
purpose: "docs/ 配下の Markdown 文書の Frontmatter・リンク・文字コードを自動検証する手順を示すため"
related_documents:
  - "ドキュメントガバナンス（相対パス: ../README.md）"
  - "設計書テンプレート運用ガイド（相対パス: ../design/TEMPLATE/README.md）"
  - "Mermaid CI 検証運用（相対パス: ./mermaid_ci_validation.md）"
---

# ドキュメント検証スクリプト運用

## 目的

`docs/` 配下の Markdown が [ドキュメントガバナンス](../README.md) の規約を満たしているかを、PR の CI で自動確認する。Mermaid 図の検証は [Mermaid CI 検証運用](./mermaid_ci_validation.md) が担当し、本書はそれ以外のメタデータとリンクを対象とする。

## 実行方法

Python 3.9 以降の標準ライブラリのみで動作する。追加パッケージは不要。プロジェクト本体の言語には依存せず、ローカルで実行する場合だけ Python が必要になる。

```bash
# docs/ 全体を検証（bash / PowerShell 共通。環境により python3 を使う）
python .github/scripts/validate_docs.py docs

# 特定のファイル・ディレクトリのみ検証
python .github/scripts/validate_docs.py docs/setup
```

終了コードは、エラーがあれば `1`、なければ `0`、対象パスが存在しなければ `2` となる。警告のみの場合は `0` で終了する。

## CI での実行

専用ワークフロー `.github/workflows/validate-docs.yml` が実行する。Mermaid 検証と同じ構成で、Python はジョブ内でのみ使用する。

- 契機: `main` / `develop` への push と PR で、`docs/**/*.md`、検証スクリプト、ワークフロー自体が変更された場合。手動実行（`workflow_dispatch`）も可能。
- `.github/workflows/ci.yml` の契約（`.github/ci.sh` または Makefile の `ci` ターゲット）は、プロジェクト固有の言語別チェックのために空けてある。ドキュメント検証は、この契約を使わない。

## 検査項目

| 区分   | 検査内容                                                                                                 | 違反時 |
| :----- | :------------------------------------------------------------------------------------------------------- | :----- |
| 文字   | UTF-8 BOM なし、LF 改行                                                                                  | エラー |
| 必須   | Frontmatter の必須キー（`docs/README.md` §6）                                                            | エラー |
| 値     | `document_role`、`status` の許容値（ADR は `proposed` / `accepted` / `rejected` も許容）                 | エラー |
| 整合   | BD/DD/DS/IF の `document_type` と `ssot_scope` の組み合わせ、BD/DD/DS/IF/ENV の `document_number` の有無 | エラー |
| 日付   | `YYYY-MM-DD` 形式、実在する日付、`updated_at` が `created_at` 以降（テンプレートは `YYYY-MM-DD` を許容） | エラー |
| リンク | `related_documents` の相対パスの実在（`...` を含む未作成文書のプレースホルダは対象外）                   | エラー |
| 旧文書 | Frontmatter が無い文書                                                                                   | 警告   |
| 種別   | `docs/README.md` の TYPE_CODE 表に未登録の `document_type`                                               | 警告   |
| 置換   | `TEMPLATE/` 以外の文書の Frontmatter に残った `[ ... ]` 形式のプレースホルダ                             | 警告   |
| コード | 設計書（BD/DD/DS/IF）本文の `mermaid` 以外のコードフェンス（行番号つき）。手順書は対象外                 | 警告   |

## 制約

- 検査対象は Frontmatter、`related_documents` のリンク、設計書のコードフェンスに限る。本文中の未置換プレースホルダは、説明文との区別がつかないため検査しない。作成完了チェックで人が確認する。
- Frontmatter の解析は簡易実装である。ネストしたマップや複数行文字列は扱わない。
- 検査ルールを変更する場合は、先に `docs/README.md` の規約を更新し、その後にスクリプトの定数を更新する。
