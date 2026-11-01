#!/usr/bin/env python3
"""docs/ 配下の Markdown 文書を検証する（標準ライブラリのみ）。

検査内容は docs/README.md（文書ガバナンス）と docs/design/TEMPLATE/README.md に準拠する。

- UTF-8 BOM なし / LF 改行
- Frontmatter 必須キー、document_role / status の許容値
- 設計書（BD/DD/DS/IF）の document_type と ssot_scope の組み合わせ
- 日付形式（YYYY-MM-DD）と updated_at >= created_at
- related_documents に書かれた相対パスの実在
- 実文書（TEMPLATE 以外）に残った未置換プレースホルダ

使い方:
    python .github/scripts/validate_docs.py            # docs/ 全体
    python .github/scripts/validate_docs.py docs/setup # 指定パスのみ

終了コード: エラーがあれば 1、なければ 0（警告のみは 0）。
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
import re
import sys

REQUIRED_KEYS = (
    "title",
    "document_type",
    "document_role",
    "ssot_scope",
    "status",
    "version",
    "created_at",
    "updated_at",
    "author",
    "owner",
    "purpose",
    "related_documents",
)
DOCUMENT_ROLES = {"requirement", "normative", "rationale", "procedure", "evidence", "reference"}
STATUSES = {"draft", "review", "approved", "superseded", "deprecated"}
# ADR は意思決定の状態を持つため、独自の状態値を追加で許容する。
ADR_EXTRA_STATUSES = {"proposed", "accepted", "rejected"}
# 設計書4種の document_type -> 期待する ssot_scope
DESIGN_SCOPES = {
    "basic_design": "system_architecture",
    "detailed_design": "module_behavior",
    "data_structure_specification": "persistent_data",
    "interface_specification": "external_interface",
}
# document_number が必須の形式文書
NUMBER_REQUIRED_TYPES = set(DESIGN_SCOPES) | {"environment_specification"}
# docs/README.md の TYPE_CODE 表に登録済みの document_type
KNOWN_TYPES = NUMBER_REQUIRED_TYPES | {
    "operation_procedure",
    "test_plan_report",
    "architecture_decision_record",
    "issue_record",
    "environment_setup_procedure",
    "document_governance",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_PLACEHOLDER = "YYYY-MM-DD"
RELPATH_RE = re.compile(r"相対パス[:：]\s*([^）)\s]+)")
KEY_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$")
LIST_ITEM_RE = re.compile(r"^\s+-\s+(.*)$")
QUOTED_RE = re.compile(r'^"(.*)"\s*(?:#.*)?$')
FENCE_OPEN_RE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
# 設計書で許可するコードフェンスの言語（図のみ）
ALLOWED_FENCE_LANGUAGES = {"mermaid"}


@dataclass
class Result:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def error(self, path: Path, message: str) -> None:
        self.errors.append(f"{path.as_posix()}: ERROR: {message}")

    def warn(self, path: Path, message: str) -> None:
        self.warnings.append(f"{path.as_posix()}: WARN: {message}")


def _unquote(raw: str) -> str:
    raw = raw.strip()
    quoted = QUOTED_RE.match(raw)
    if quoted:
        return quoted.group(1)
    # 引用符なしの値は末尾コメントを除去する。
    return re.sub(r"\s+#.*$", "", raw).strip()


def parse_frontmatter(text: str) -> dict[str, object] | None:
    """先頭の Frontmatter を簡易解析する。無ければ None、閉じ忘れは ValueError。"""
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        return None
    data: dict[str, object] = {}
    current: str | None = None
    for line in lines[1:]:
        if line.strip() == "---":
            return data
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        item = LIST_ITEM_RE.match(line)
        if item and current is not None:
            value = data.setdefault(current, [])
            if isinstance(value, list):
                value.append(_unquote(item.group(1)))
            continue
        key = KEY_RE.match(line)
        if key:
            current = key.group(1)
            rest = key.group(2).strip()
            data[current] = [] if rest == "" else _unquote(rest)
    raise ValueError("Frontmatter が '---' で閉じられていません")


def _is_placeholder(value: str) -> bool:
    return DATE_PLACEHOLDER in value or bool(re.search(r"\[[^\]]*\]", value))


def _check_dates(path: Path, data: dict[str, object], is_template: bool, result: Result) -> None:
    parsed: dict[str, date] = {}
    for key in ("created_at", "updated_at"):
        value = data.get(key)
        if not isinstance(value, str):
            continue
        if value == DATE_PLACEHOLDER and is_template:
            continue
        if not DATE_RE.match(value):
            result.error(path, f"{key} は YYYY-MM-DD 形式で指定してください: {value!r}")
            continue
        try:
            parsed[key] = datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            result.error(path, f"{key} が存在しない日付です: {value!r}")
    if len(parsed) == 2 and parsed["updated_at"] < parsed["created_at"]:
        result.error(path, "updated_at が created_at より前になっています")


def _check_related_documents(path: Path, data: dict[str, object], result: Result) -> None:
    related = data.get("related_documents")
    if not isinstance(related, list) or not related:
        result.error(path, "related_documents は1件以上の配列で指定してください")
        return
    for entry in related:
        match = RELPATH_RE.search(entry)
        if not match:
            continue
        rel = match.group(1).split("#", 1)[0]
        # 未作成文書を示すプレースホルダ（./... など）は検査しない。
        if "..." in rel.replace("../", "").replace("./", "") or "[" in rel:
            continue
        if not (path.parent / rel).exists():
            result.error(path, f"related_documents の相対パスが存在しません: {rel}")


def _check_code_fences(path: Path, text: str, result: Result) -> None:
    """設計書（BD/DD/DS/IF）本文の mermaid 以外のコードフェンスを警告する。

    設計書では具象ソースコードの貼付を禁止している（設計書テンプレート運用ガイド §6②）。
    手順書など設計書以外の文書は対象外。
    """
    fence: str | None = None
    for number, line in enumerate(text.split("\n"), start=1):
        if fence is None:
            opening = FENCE_OPEN_RE.match(line)
            if opening:
                fence = opening.group(1)
                language = opening.group(2).strip().split(" ", 1)[0].lower()
                if language not in ALLOWED_FENCE_LANGUAGES:
                    shown = language or "言語指定なし"
                    result.warn(path, f"{number}行目: 設計書に mermaid 以外のコードフェンスがあります（{shown}）")
        elif re.match(rf"^\s*{re.escape(fence[0])}{{{len(fence)},}}\s*$", line):
            fence = None


def validate_file(path: Path, result: Result) -> None:
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        result.error(path, "UTF-8 BOM が含まれています（BOMなしにしてください）")
    if b"\r" in raw:
        result.error(path, "CR（CRLF）改行が含まれています（LFにしてください）")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        result.error(path, "UTF-8 として読み取れません")
        return

    try:
        data = parse_frontmatter(text.replace("\r\n", "\n"))
    except ValueError as exc:
        result.error(path, str(exc))
        return
    if data is None:
        result.warn(path, "Frontmatter がありません（既存文書は更新時に適用）")
        return

    is_template = "TEMPLATE" in path.parts
    missing = [key for key in REQUIRED_KEYS if key not in data or data[key] in ("", [])]
    for key in missing:
        result.error(path, f"必須キー {key} がありません")

    doc_type = data.get("document_type") if isinstance(data.get("document_type"), str) else None
    if doc_type is not None:
        if doc_type not in KNOWN_TYPES:
            result.warn(path, f"document_type {doc_type!r} は docs/README.md の表に未登録です")
        if doc_type in NUMBER_REQUIRED_TYPES and not data.get("document_number"):
            result.error(path, f"{doc_type} では document_number が必須です")
        expected_scope = DESIGN_SCOPES.get(doc_type)
        if expected_scope and data.get("ssot_scope") != expected_scope:
            result.error(path, f"{doc_type} の ssot_scope は {expected_scope!r} にしてください")

    role = data.get("document_role")
    if isinstance(role, str) and role not in DOCUMENT_ROLES:
        result.error(path, f"document_role が不正です: {role!r}")

    status = data.get("status")
    allowed = STATUSES | (ADR_EXTRA_STATUSES if doc_type == "architecture_decision_record" else set())
    if isinstance(status, str) and status not in allowed:
        result.error(path, f"status が不正です: {status!r}")

    _check_dates(path, data, is_template, result)
    if "related_documents" in data:
        _check_related_documents(path, data, result)
    if doc_type in DESIGN_SCOPES:
        _check_code_fences(path, text, result)

    if not is_template:
        for key, value in data.items():
            if key in ("created_at", "updated_at") or not isinstance(value, str):
                continue
            if _is_placeholder(value):
                result.warn(path, f"{key} に未置換のプレースホルダが残っています: {value!r}")


def collect_files(targets: list[Path]) -> list[Path]:
    files: list[Path] = []
    for target in targets:
        if target.is_dir():
            files.extend(sorted(target.rglob("*.md")))
        elif target.suffix == ".md" and target.exists():
            files.append(target)
    return files


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="docs/ 配下の Markdown 文書を検証する")
    parser.add_argument("paths", nargs="*", default=["docs"], help="検証対象のファイルまたはディレクトリ")
    args = parser.parse_args(argv)

    targets = [Path(p) for p in args.paths]
    missing = [t for t in targets if not t.exists()]
    if missing:
        print(f"対象が存在しません: {', '.join(t.as_posix() for t in missing)}", file=sys.stderr)
        return 2

    files = collect_files(targets)
    result = Result()
    for file in files:
        validate_file(file, result)

    for line in result.warnings + result.errors:
        print(line)
    print(f"検証 {len(files)} 件 / エラー {len(result.errors)} 件 / 警告 {len(result.warnings)} 件")
    return 1 if result.errors else 0


if __name__ == "__main__":
    sys.exit(main())
