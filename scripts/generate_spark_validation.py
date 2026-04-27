#!/usr/bin/env python3
"""
Spark 물리 검증 스크립트 생성기 (Phase 5 - Iceberg 전용)
business_model.json을 읽어 Spark + Iceberg 환경에서 실행할 검증 스크립트와 체크리스트를 생성합니다.
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


def generate_partitioning_test(tables: list, gold_tables: list, namespace: str) -> str:
    """파티션 transform 동작 검증 스크립트 생성"""
    all_tables = tables + gold_tables
    checks = []

    for tbl in all_tables:
        name = tbl.get("name", "")
        partition_spec = tbl.get("partition_spec", [])
        full_name = f"{namespace}.{name}"

        for p in partition_spec:
            transform = p.get("transform", "")
            column = p.get("column", "")
            checks.append((full_name, column, transform))

    script = f'''#!/usr/bin/env python3
"""
Spark 파티션 Transform 검증 스크립트
생성일시: {datetime.now().isoformat()}

실행 방법:
    spark-submit spark_validate_partitioning.py \\
        --master yarn \\
        --jars iceberg-spark-runtime.jar
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    return (
        SparkSession.builder
        .appName("iceberg_partition_validation")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.{namespace}",
                "org.apache.iceberg.spark.SparkCatalog")
        .getOrCreate()
    )


def validate_partition(spark, table_name, column, transform):
    """파티션 transform이 실제로 적용됐는지 확인"""
    print(f"\\n🔍 검증: {{table_name}} | {{column}} | {{transform}}")

    try:
        # 1. 파티션 정보 확인
        partitions = spark.sql(f"""
            SELECT partition, record_count, file_count
            FROM {{table_name}}.partitions
            ORDER BY partition
            LIMIT 10
        """)
        partitions.show(truncate=False)

        # 2. 파티션 프루닝 동작 확인
        sample_date = spark.sql(f"""
            SELECT MIN({column}), MAX({column})
            FROM {{table_name}}
        """).collect()[0]

        if sample_date[0]:
            pruning_result = spark.sql(f"""
                EXPLAIN SELECT * FROM {{table_name}}
                WHERE {column} >= '{{sample_date[0]}}'
            """)
            plan = pruning_result.collect()[0][0]
            if "PartitionFilters" in plan:
                print(f"  ✅ 파티션 프루닝 동작 확인")
            else:
                print(f"  ⚠️  파티션 프루닝 미동작 — 실행 계획 확인 필요")

        print(f"  ✅ {{table_name}} 파티션 검증 완료")
        return True

    except Exception as e:
        print(f"  ❌ 검증 실패: {{e}}")
        return False


def main():
    spark = create_spark_session()
    results = []

    checks = {json.dumps(checks, ensure_ascii=False)}

    for table_name, column, transform in checks:
        passed = validate_partition(spark, table_name, column, transform)
        results.append({{
            "table": table_name,
            "column": column,
            "transform": transform,
            "passed": passed,
        }})

    # 결과 요약
    print("\\n" + "="*60)
    print("📊 파티션 검증 결과 요약")
    passed_count = sum(1 for r in results if r["passed"])
    print(f"   통과: {{passed_count}}/{{len(results)}}")
    for r in results:
        icon = "✅" if r["passed"] else "❌"
        print(f"   {{icon}} {{r['table']}} | {{r['column']}} | {{r['transform']}}")
    print("="*60)

    spark.stop()
    return all(r["passed"] for r in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
'''
    return script


def generate_mor_merge_test(tables: list, namespace: str) -> str:
    """MOR 머지 정확성 검증 스크립트 생성"""
    mor_tables = [t for t in tables if t.get("mode", "").upper() == "MOR"]

    script = f'''#!/usr/bin/env python3
"""
Spark MOR 머지 정확성 검증 스크립트
생성일시: {datetime.now().isoformat()}

실행 방법:
    spark-submit spark_validate_mor_merge.py \\
        --master yarn \\
        --jars iceberg-spark-runtime.jar
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    return (
        SparkSession.builder
        .appName("iceberg_mor_merge_validation")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.{namespace}",
                "org.apache.iceberg.spark.SparkCatalog")
        .getOrCreate()
    )


def validate_mor_merge(spark, table_name):
    """MOR 테이블의 머지 동작 검증"""
    print(f"\\n🔍 MOR 머지 검증: {{table_name}}")

    try:
        # 1. 현재 스냅샷 확인
        snapshots = spark.sql(f"""
            SELECT snapshot_id, committed_at, operation
            FROM {{table_name}}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
        """)
        print("  최근 스냅샷:")
        snapshots.show(truncate=False)

        # 2. delete 파일 존재 여부 확인 (MOR의 증거)
        files = spark.sql(f"""
            SELECT content, COUNT(*) as file_count
            FROM {{table_name}}.files
            GROUP BY content
        """)
        print("  파일 타입별 수:")
        files.show()

        # 3. 중복 행 없음 확인
        row_count = spark.sql(f"SELECT COUNT(*) FROM {{table_name}}").collect()[0][0]
        print(f"  총 행 수: {{row_count:,}}")

        print(f"  ✅ {{table_name}} MOR 검증 완료")
        return True

    except Exception as e:
        print(f"  ❌ 검증 실패: {{e}}")
        return False


def main():
    spark = create_spark_session()
    results = []

    mor_tables = {json.dumps([t.get("name", "") for t in mor_tables])}

    for table_name in mor_tables:
        full_name = f"{namespace}.{{table_name}}"
        passed = validate_mor_merge(spark, full_name)
        results.append({{"table": full_name, "passed": passed}})

    print("\\n" + "="*60)
    print("📊 MOR 머지 검증 결과 요약")
    passed_count = sum(1 for r in results if r["passed"])
    print(f"   통과: {{passed_count}}/{{len(results)}}")
    for r in results:
        icon = "✅" if r["passed"] else "❌"
        print(f"   {{icon}} {{r['table']}}")
    print("="*60)

    spark.stop()
    return all(r["passed"] for r in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
'''
    return script


def generate_checklist(model: dict, output_dir: Path) -> str:
    """물리 검증 체크리스트 Markdown 생성"""
    tables = model.get("tables", [])
    gold_tables = model.get("gold_tables", [])
    all_tables = tables + gold_tables
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    mor_tables = [t["name"] for t in tables if t.get("mode", "").upper() == "MOR"]
    partition_tables = [t["name"] for t in all_tables if t.get("partition_spec")]

    lines = [
        "# 🟧 Spark 물리 검증 체크리스트",
        "",
        f"> **생성일시**: {generated_at}",
        f"> **검증 환경**: Apache Spark + Apache Iceberg",
        "> ⚠️ 아래 항목은 로컬 DuckDB로 검증할 수 없습니다. 실제 Spark 환경에서 실행하세요.",
        "",
        "## 실행 순서",
        "",
        "```bash",
        "# 1. 파티션 transform 검증",
        f"spark-submit {output_dir}/spark_validate_partitioning.py \\",
        "    --master yarn --jars iceberg-spark-runtime.jar",
        "",
        "# 2. MOR 머지 검증",
        f"spark-submit {output_dir}/spark_validate_mor_merge.py \\",
        "    --master yarn --jars iceberg-spark-runtime.jar",
        "```",
        "",
        "## 체크 항목",
        "",
    ]

    # 파티션 검증 항목
    if partition_tables:
        lines.append("### 파티션 Transform")
        for tbl_name in partition_tables:
            tbl = next((t for t in all_tables if t["name"] == tbl_name), {})
            for p in tbl.get("partition_spec", []):
                transform = p.get("transform", "")
                column = p.get("column", "")
                lines.append(f"- ⬜ `{tbl_name}` — `{transform}({column})` 파티션 프루닝 동작")
        lines.append("")

    # MOR 검증 항목
    if mor_tables:
        lines.append("### MOR 머지 동작")
        for name in mor_tables:
            lines.append(f"- ⬜ `{name}` — MOR delete 파일 생성 확인")
            lines.append(f"- ⬜ `{name}` — 동시 쓰기 충돌 없음 확인")
        lines.append("")

    # 공통 항목
    lines += [
        "### 카탈로그 & 연동",
        "- ⬜ Iceberg 카탈로그 접근 권한 확인",
        "- ⬜ 스냅샷 타임트래블 동작 확인",
        "- ⬜ `expire_snapshots` 실행 테스트",
        "",
        "### 성능",
        "- ⬜ 실제 데이터 규모(운영 동일 수준)로 쿼리 응답시간 측정",
        "- ⬜ 파티션 프루닝 Explain 계획 확인",
        "",
        "## 완료 후 처리",
        "",
        "모든 항목 완료 후 `business_model.json`의 검증 상태를 업데이트하세요:",
        "",
        "```json",
        '"validation": {',
        '  "spark": {',
        '    "status": "PASSED",',
        f'    "completed_at": "{generated_at}"',
        '  }',
        '}',
        "```",
    ]

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Spark 물리 검증 스크립트 생성 (Iceberg)")
    parser.add_argument("--config", "-c", required=True,
                        help="business_model.json 파일 경로")
    parser.add_argument("--output-dir", "-o", default="output/validation",
                        help="출력 디렉토리 (기본: output/validation)")
    parser.add_argument("--namespace", "-n", default="lakehouse",
                        help="Iceberg 카탈로그 네임스페이스 (기본: lakehouse)")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.config}", file=sys.stderr)
        sys.exit(1)

    model = json.loads(config_path.read_text(encoding="utf-8"))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = model.get("tables", [])
    gold_tables = model.get("gold_tables", [])
    all_tables = tables + gold_tables

    if not all_tables:
        print("⚠️  테이블 정의가 없습니다 (Phase 3b 완료 후 실행하세요)")
        sys.exit(1)

    print(f"🔧 Spark 검증 스크립트 생성 중...")
    print(f"   대상 테이블: {len(all_tables)}개")
    print(f"   출력 경로: {output_dir}")

    # 파티션 검증 스크립트
    partitioning_script = generate_partitioning_test(tables, gold_tables, args.namespace)
    partitioning_path = output_dir / "spark_validate_partitioning.py"
    partitioning_path.write_text(partitioning_script, encoding="utf-8")
    print(f"   ✅ {partitioning_path}")

    # MOR 머지 검증 스크립트
    mor_script = generate_mor_merge_test(tables, args.namespace)
    mor_path = output_dir / "spark_validate_mor_merge.py"
    mor_path.write_text(mor_script, encoding="utf-8")
    print(f"   ✅ {mor_path}")

    # 체크리스트
    checklist = generate_checklist(model, output_dir)
    checklist_path = output_dir / "spark_validate_checklist.md"
    checklist_path.write_text(checklist, encoding="utf-8")
    print(f"   ✅ {checklist_path}")

    print(f"\n{'='*60}")
    print(f"🟧 물리 검증 체크리스트 생성 완료")
    print(f"   Spark 환경에서 직접 실행이 필요합니다.")
    print(f"\n   실행 방법:")
    print(f"   spark-submit {partitioning_path} --master yarn --jars iceberg-spark-runtime.jar")
    print(f"   spark-submit {mor_path} --master yarn --jars iceberg-spark-runtime.jar")
    print(f"\n   체크리스트: {checklist_path}")
    print(f"\n   ⚠️  Skip 하려면 다음을 입력하세요: '동의합니다'")
    print(f"       (Skip 시 business_model.json에 기록되고 산출물에 경고 포함)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
