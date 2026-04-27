#!/usr/bin/env python3
"""
설정 파일 병합 스크립트 (Phase 6 - Option Z 구현)
business_model.json(논리+물리 통합)을 기존 스크립트가 읽는 design_config.json으로 변환합니다.
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


def transform_term_mappings(term_mappings: list) -> list:
    """term_mappings → design_config의 column_mappings 형식으로 변환"""
    column_mappings = []
    for m in term_mappings:
        if not m.get("source_table") or not m.get("source_column"):
            continue
        column_mappings.append({
            "source_table": m["source_table"],
            "source_column": m["source_column"],
            "transform": m.get("sql_expression", "direct"),
            "target_table": m.get("target_table", ""),
            "target_column": m.get("target_column", m.get("business_term", "")),
            "type": m.get("type", "STRING"),
        })
    return column_mappings


def build_design_config(model: dict) -> dict:
    """
    business_model.json → design_config.json 변환
    기존 generate_artifacts.py, sandbox_validate.py 등과 호환되는 형식 출력
    """
    # 검증 상태 요약
    validation = model.get("validation", {})
    duckdb = validation.get("duckdb", {})
    spark = validation.get("spark", {})

    validation_summary = {
        "duckdb_status": duckdb.get("status", "PENDING"),
        "duckdb_timestamp": duckdb.get("timestamp", ""),
        "spark_status": spark.get("status", "PENDING"),
        "spark_skipped_by_user": spark.get("skipped_by_user", False),
        "spark_skip_acknowledged_at": spark.get("skip_acknowledged_at", ""),
    }

    # 기본 lineage: sources → tables → gold_tables
    lineage = model.get("lineage", [])
    if not lineage:
        sources = model.get("sources", [])
        tables = model.get("tables", [])
        gold_tables = model.get("gold_tables", [])
        for src in sources:
            for tbl in tables:
                lineage.append({
                    "from": src["name"],
                    "to": tbl["name"],
                    "transform": "CDC + Cleanse",
                })
        for tbl in tables:
            for gold in gold_tables:
                lineage.append({
                    "from": tbl["name"],
                    "to": gold["name"],
                    "transform": "GROUP BY + AGG",
                })

    # column_mappings: 기존 필드 또는 term_mappings에서 변환
    column_mappings = model.get("column_mappings") or \
                      transform_term_mappings(model.get("term_mappings", []))

    design_config = {
        # 비즈니스 섹션 (business_model.json에서 직접 복사)
        "business_context": model.get("business_context", ""),
        "freshness": model.get("freshness", "daily"),
        "kpis": model.get("kpis", []),
        "dimensions": model.get("dimensions", []),
        "sources": model.get("sources", []),

        # 물리 섹션 (lakehouse-[format] 스킬이 추가한 내용)
        "tables": model.get("tables", []),
        "gold_tables": model.get("gold_tables", []),
        "lineage": lineage,
        "column_mappings": column_mappings,

        # 메타 섹션
        "user_approved": model.get("user_approved", False),
        "mode": model.get("mode", "CREATE"),
        "format": model.get("format", "iceberg"),
        "abstract_strategy": model.get("abstract_strategy", {}),
        "validation": validation_summary,
        "generated_at": datetime.now().isoformat(),
    }

    return design_config


def validate_required_fields(model: dict) -> list:
    """design_config 생성 전 필수 필드 검증"""
    errors = []

    if not model.get("business_context"):
        errors.append("business_context 가 없습니다 (Phase 1 미완료)")

    if not model.get("kpis"):
        errors.append("kpis 가 없습니다 (Phase 1 미완료)")

    if not model.get("sources"):
        errors.append("sources 가 없습니다 (Phase 2 미완료)")

    duckdb_status = model.get("validation", {}).get("duckdb", {}).get("status", "PENDING")
    if duckdb_status != "PASSED":
        errors.append(f"DuckDB 논리 검증이 완료되지 않았습니다 (현재: {duckdb_status})")

    spark = model.get("validation", {}).get("spark", {})
    spark_status = spark.get("status", "PENDING")
    if spark_status == "PENDING" and not spark.get("skipped_by_user", False):
        errors.append(
            "Spark 물리 검증이 완료되지 않았습니다.\n"
            "  실행: python scripts/generate_spark_validation.py --config business_model.json\n"
            "  또는 Skip 동의 후 business_model.json의 validation.spark.skipped_by_user = true 설정"
        )

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="business_model.json → design_config.json 변환 (Option Z)"
    )
    parser.add_argument("--input", "-i", required=True,
                        help="business_model.json 파일 경로")
    parser.add_argument("--output", "-o", default="design_config.json",
                        help="출력 design_config.json 파일 경로 (기본: design_config.json)")
    parser.add_argument("--force", "-f", action="store_true",
                        help="검증 오류가 있어도 강제 생성")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"📖 business_model.json 읽는 중: {args.input}")
    model = json.loads(input_path.read_text(encoding="utf-8"))

    # 필수 필드 검증
    errors = validate_required_fields(model)
    if errors:
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"⚠️  검증 오류 발견:", file=sys.stderr)
        for err in errors:
            print(f"   - {err}", file=sys.stderr)

        if not args.force:
            print(f"\n  강제 생성: --force 옵션 추가", file=sys.stderr)
            print(f"{'='*60}", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"\n  --force 옵션으로 강제 생성합니다.", file=sys.stderr)
            print(f"{'='*60}", file=sys.stderr)

    # 변환 실행
    print("🔄 design_config.json 변환 중...")
    design_config = build_design_config(model)

    # Spark Skip 경고 삽입
    if model.get("validation", {}).get("spark", {}).get("skipped_by_user"):
        design_config["_warnings"] = [
            "⚠️ Spark 물리 검증이 사용자 임의 승인으로 Skip됐습니다.",
            "운영 배포 전 output/validation/ 의 스크립트를 Spark 환경에서 실행하세요.",
        ]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(design_config, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"\n{'='*60}")
    print(f"✅ design_config.json 생성 완료: {args.output}")
    print(f"   테이블:      Silver {len(design_config['tables'])}개 / Gold {len(design_config['gold_tables'])}개")
    print(f"   소스:        {len(design_config['sources'])}개")
    print(f"   🟦 DuckDB:   {design_config['validation']['duckdb_status']}")
    print(f"   🟧 Spark:    {design_config['validation']['spark_status']}")
    if design_config["validation"]["spark_skipped_by_user"]:
        print(f"   ⚠️  Spark 검증 Skip — 운영 배포 전 별도 확인 필요")
    print(f"{'='*60}")

    print(f"\n다음 단계:")
    print(f"   python scripts/generate_artifacts.py --config {args.output} --output-dir ./output")


if __name__ == "__main__":
    main()
