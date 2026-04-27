#!/usr/bin/env python3
"""
비즈니스 용어 사전(Glossary) 생성 스크립트 (Phase 4)
business_model.json의 term_mappings를 읽어 Markdown 용어 사전을 생성합니다.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ── venv 강제 실행 가드 ──────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from venv_guard import ensure_venv
ensure_venv()
# ────────────────────────────────────────────────────────────────


def build_glossary_markdown(model: dict) -> str:
    """business_model.json → Markdown 용어 사전 변환"""
    lines = []

    # 헤더
    business_context = model.get("business_context", "데이터 레이크하우스")
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines += [
        f"# 비즈니스 용어 사전 (Term Glossary)",
        f"",
        f"> **프로젝트**: {business_context}",
        f"> **생성일시**: {generated_at}",
        f"> ⚠️ 이 문서는 자동 생성됩니다. 수정은 `business_model.json`의 `term_mappings`에서 하세요.",
        f"",
    ]

    # 검증 상태 배너
    validation = model.get("validation", {})
    duckdb_status = validation.get("duckdb", {}).get("status", "PENDING")
    spark_status = validation.get("spark", {}).get("status", "PENDING")
    spark_skipped = validation.get("spark", {}).get("skipped_by_user", False)

    lines += [
        f"## 검증 상태",
        f"",
        f"| 검증 종류 | 상태 | 비고 |",
        f"|---|---|---|",
        f"| 🟦 논리 검증 (DuckDB) | `{duckdb_status}` | 비즈니스 로직 검증 |",
    ]
    spark_note = "사용자 임의 승인으로 Skip" if spark_skipped else "Spark 환경에서 실행 필요"
    lines.append(f"| 🟧 물리 검증 (Spark) | `{spark_status}` | {spark_note} |")
    lines.append("")

    # KPI 섹션
    kpis = model.get("kpis", [])
    if kpis:
        lines += ["## 핵심 KPI (Key Performance Indicators)", ""]
        lines += ["| KPI 명 | 산식 | 단위 |", "|---|---|---|"]
        for kpi in kpis:
            name = kpi.get("name", "")
            formula = kpi.get("formula", "")
            unit = kpi.get("unit", "")
            lines.append(f"| **{name}** | `{formula}` | {unit} |")
        lines.append("")

    # 용어 매핑 섹션
    mappings = model.get("term_mappings", [])
    if mappings:
        lines += [
            "## 비즈니스 용어 ↔ 데이터 컬럼 매핑",
            "",
            "| 비즈니스 용어 | 소스 테이블 | 소스 컬럼 | 변환 로직 | 확인일시 |",
            "|---|---|---|---|---|",
        ]
        for m in mappings:
            term = m.get("business_term", "")
            src_table = m.get("source_table") or "(파생)"
            src_col = m.get("source_column") or "(파생)"
            sql_expr = m.get("sql_expression", "")
            confirmed_at = m.get("confirmed_at", "")[:10] if m.get("confirmed_at") else "-"
            is_derived = m.get("is_derived", False)

            # 코드값 매핑이 있는 경우 표시
            value_mapping = m.get("value_mapping")
            if value_mapping:
                mapping_str = " / ".join(f"{k}→{v}" for k, v in value_mapping.items())
                sql_expr = f"`{sql_expr}` ({mapping_str})"
            elif sql_expr:
                prefix = "파생: " if is_derived else ""
                sql_expr = f"`{prefix}{sql_expr}`"

            lines.append(f"| **{term}** | {src_table} | {src_col} | {sql_expr} | {confirmed_at} |")
        lines.append("")

    # 차원 섹션
    dimensions = model.get("dimensions", [])
    if dimensions:
        lines += ["## 분석 차원 (Dimensions)", ""]
        for dim in dimensions:
            lines.append(f"- `{dim}`")
        lines.append("")

    # 소스 섹션
    sources = model.get("sources", [])
    if sources:
        lines += ["## 소스 데이터 목록", ""]
        for src in sources:
            name = src.get("name", "")
            origin = src.get("origin", "")
            method = src.get("load_method", "")
            freq = src.get("update_frequency", "")
            issues = src.get("quality_issues", [])

            lines.append(f"### {name}")
            lines += [
                f"- **출처**: {origin}",
                f"- **적재 방식**: {method}",
                f"- **갱신 주기**: {freq}",
            ]
            if issues:
                lines.append("- **품질 이슈**:")
                for issue in issues:
                    lines.append(f"  - ⚠️ {issue}")
            lines.append("")

    # Spark Skip 경고
    if spark_skipped:
        unchecked = validation.get("spark", {}).get("checklist", [])
        lines += [
            "---",
            "",
            "## ⚠️ Spark 물리 검증 미완료 경고",
            "",
            "이 문서는 Spark 물리 검증이 완료되지 않은 상태에서 생성됐습니다.",
            "운영 배포 전 반드시 아래 항목을 Spark 환경에서 확인하세요:",
            "",
        ]
        for item in unchecked:
            lines.append(f"- ⬜ {item}")
        if not unchecked:
            lines += [
                "- ⬜ 파티션 transform 동작 확인",
                "- ⬜ MOR 머지 정확성 확인",
                "- ⬜ 카탈로그 연동 확인",
            ]
        lines.append("")
        skip_time = validation.get("spark", {}).get("skip_acknowledged_at", "")
        lines.append(f"Skip 동의 일시: {skip_time}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="비즈니스 용어 사전 생성")
    parser.add_argument("--input", "-i", required=True,
                        help="business_model.json 파일 경로")
    parser.add_argument("--output", "-o", default=None,
                        help="출력 Markdown 파일 경로 (미지정 시 stdout)")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"📖 business_model.json 읽는 중: {args.input}")
    model = json.loads(input_path.read_text(encoding="utf-8"))

    term_count = len(model.get("term_mappings", []))
    kpi_count = len(model.get("kpis", []))
    print(f"   KPI: {kpi_count}개 / 용어 매핑: {term_count}개")

    markdown = build_glossary_markdown(model)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(markdown, encoding="utf-8")
        print(f"✅ 용어 사전 생성 완료: {args.output}")
    else:
        print("\n" + markdown)


if __name__ == "__main__":
    main()
