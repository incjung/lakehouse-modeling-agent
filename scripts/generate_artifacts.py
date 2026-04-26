#!/usr/bin/env python3
"""
최종 구현 아티팩트 생성 스크립트 (Phase 6)
검증된 설계 구성에서 Iceberg DDL, PySpark ETL, 운영 가이드를 생성합니다.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


def generate_iceberg_ddl(table: dict, namespace: str = "lakehouse") -> str:
    """Iceberg CREATE TABLE DDL 생성"""
    name = table["name"]
    layer = table.get("layer", "silver")
    mode = table.get("mode", "COW")
    columns = table.get("columns", [])
    partition_spec = table.get("partition_spec", [])
    sort_order = table.get("sort_order", [])
    comment = table.get("description", f"{name} table")

    # 컬럼 정의
    col_lines = []
    for col in columns:
        col_type = col.get("type", "STRING")
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        col_comment = f" COMMENT '{col.get('description', '')}'" if col.get("description") else ""
        col_lines.append(f"    {col['name']}  {col_type}{nullable}{col_comment}")

    col_def = ",\n".join(col_lines)

    # 파티션 정의
    partition_clause = ""
    if partition_spec:
        parts = []
        for p in partition_spec:
            transform = p.get("transform", "identity")
            col = p["column"]
            if transform == "identity":
                parts.append(col)
            else:
                parts.append(f"{transform}({col})")
        partition_clause = f"\nPARTITIONED BY ({', '.join(parts)})"

    # 테이블 프로퍼티
    properties = {
        "format-version": "2",
        "write.distribution-mode": "hash" if mode == "MOR" else "none",
    }
    if mode == "MOR":
        properties["write.merge.mode"] = "merge-on-read"
        properties["write.update.mode"] = "merge-on-read"
        properties["write.delete.mode"] = "merge-on-read"

    retention = table.get("retention_days")
    if retention:
        properties["history.expire.max-snapshot-age-ms"] = str(retention * 86400 * 1000)

    props_lines = ",\n    ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    props_clause = f"\nTBLPROPERTIES (\n    {props_lines}\n)"

    # 정렬 순서 (코멘트)
    sort_comment = ""
    if sort_order:
        sort_cols = ", ".join(sort_order)
        sort_comment = f"\n-- Sort Order: {sort_cols}"
        sort_comment += f"\n-- ALTER TABLE {namespace}.{name} WRITE ORDERED BY ({sort_cols});"

    ddl = f"""-- =============================================================
-- Table: {namespace}.{name}
-- Layer: {layer.upper()} | Mode: {mode}
-- Generated: {datetime.now().isoformat()}
-- =============================================================
CREATE TABLE IF NOT EXISTS {namespace}.{name} (
{col_def}
)
USING iceberg{partition_clause}{props_clause};
{sort_comment}

COMMENT ON TABLE {namespace}.{name} IS '{comment}';
"""

    # 컬럼 코멘트
    for col in columns:
        if col.get("description"):
            ddl += f"\n-- COMMENT ON COLUMN {namespace}.{name}.{col['name']} IS '{col['description']}';"

    return ddl


def generate_pyspark_etl(table: dict, sources: list, namespace: str = "lakehouse") -> str:
    """PySpark ETL 스크립트 생성"""
    name = table["name"]
    layer = table.get("layer", "silver")
    mode = table.get("mode", "COW")
    columns = table.get("columns", [])

    col_list = ", ".join(f'"{c["name"]}"' for c in columns)

    # 소스 테이블 읽기 부분
    source_reads = []
    for src in sources:
        src_name = src.get("name", "source_table")
        source_reads.append(f'''
    # Read source: {src_name}
    df_{src_name} = spark.table("{namespace}.{src_name}")''')

    source_read_code = "\n".join(source_reads) if source_reads else f'''
    # TODO: 소스 테이블을 지정하세요
    df_source = spark.table("{namespace}.source_table")'''

    write_mode = "append" if layer == "silver" else "overwrite"

    etl = f'''#!/usr/bin/env python3
"""
ETL Script: {namespace}.{name}
Layer: {layer.upper()} | Mode: {mode}
Generated: {datetime.now().isoformat()}
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("{name}_etl")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.{namespace}", "org.apache.iceberg.spark.SparkCatalog")
        .getOrCreate()
    )


def extract(spark: SparkSession):
    """데이터 추출"""
    {source_read_code}
    return df_source  # TODO: 복수 소스인 경우 딕셔너리로 반환


def transform(df):
    """데이터 변환"""
    result = df

    # TODO: 비즈니스 로직에 맞게 변환 로직을 구현하세요
    # 예시:
    # result = (
    #     df
    #     .withColumn("production_date", F.to_date("event_ts"))
    #     .withColumn("defect_rate", F.col("defect_count") / F.col("total_count") * 100)
    #     .groupBy("production_date", "line_id")
    #     .agg(
    #         F.sum("total_count").alias("total_count"),
    #         F.avg("defect_rate").alias("avg_defect_rate"),
    #     )
    # )

    # 최종 컬럼 선택
    # result = result.select({col_list})

    return result


def load(df, spark: SparkSession):
    """Iceberg 테이블에 적재"""
    (
        df.writeTo("{namespace}.{name}")
        .using("iceberg")
        .{"append" if write_mode == "append" else "overwritePartitions"}()
    )
    print(f"✅ Loaded {{df.count()}} rows into {namespace}.{name}")


def main():
    spark = create_spark_session()

    try:
        # Extract
        raw_df = extract(spark)
        print(f"📥 Extracted: {{raw_df.count()}} rows")

        # Transform
        result_df = transform(raw_df)
        print(f"🔄 Transformed: {{result_df.count()}} rows")

        # Load
        load(result_df, spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''
    return etl


def generate_ops_guide(tables: list, namespace: str = "lakehouse") -> str:
    """운영 가이드(Day 2 Ops) 생성"""
    guide = f"""# 운영 가이드 (Day 2 Operations)

생성일: {datetime.now().strftime("%Y-%m-%d")}
네임스페이스: {namespace}

## 1. Compaction (파일 병합)

읽기 성능 유지를 위해 정기적으로 Small File을 병합합니다.

"""

    for tbl in tables:
        name = tbl["name"]
        schedule = tbl.get("compaction_schedule", "weekly")
        guide += f"""### {name}
```sql
-- 실행 주기: {schedule}
CALL {namespace}.system.rewrite_data_files(
    table => '{namespace}.{name}',
    strategy => 'sort',
    sort_order => '{", ".join(tbl.get("sort_order", []))}'
);
```

"""

    guide += """## 2. Snapshot 관리

오래된 스냅샷을 정리하여 메타데이터 파일 비대화를 방지합니다.

"""

    for tbl in tables:
        name = tbl["name"]
        retention = tbl.get("retention_days", 365)
        guide += f"""### {name}
```sql
-- 보관 기간: {retention}일
CALL {namespace}.system.expire_snapshots(
    table => '{namespace}.{name}',
    older_than => TIMESTAMP '{datetime.now().strftime("%Y-%m-%d")}' - INTERVAL {min(retention, 30)} DAYS,
    retain_last => 10
);
```

"""

    guide += """## 3. Z-Order 최적화

다차원 조회가 빈번한 테이블에 적용하여 Data Skipping 효율을 높입니다.

"""

    for tbl in tables:
        sort_cols = tbl.get("sort_order", [])
        if sort_cols:
            name = tbl["name"]
            guide += f"""### {name}
```sql
CALL {namespace}.system.rewrite_data_files(
    table => '{namespace}.{name}',
    strategy => 'sort',
    sort_order => 'zorder({", ".join(sort_cols)})'
);
```

"""

    guide += """## 4. 모니터링 쿼리

```sql
-- 테이블별 스냅샷 수 확인
SELECT * FROM {namespace}.{table_name}.snapshots ORDER BY committed_at DESC LIMIT 10;

-- 파일 크기 분포 확인
SELECT
    file_format,
    COUNT(*) as file_count,
    SUM(file_size_in_bytes) / 1024 / 1024 as total_mb,
    AVG(file_size_in_bytes) / 1024 / 1024 as avg_mb,
    MIN(file_size_in_bytes) / 1024 / 1024 as min_mb,
    MAX(file_size_in_bytes) / 1024 / 1024 as max_mb
FROM {namespace}.{table_name}.files
GROUP BY file_format;
```

## 5. 권장 운영 스케줄

| 작업 | 주기 | 시간 | 비고 |
|---|---|---|---|
| Compaction (Silver) | 주 1회 | 일요일 02:00 | 읽기 성능 최적화 |
| Compaction (Gold) | 월 1회 | 매월 1일 03:00 | COW 테이블은 빈도 낮춤 |
| Snapshot Expire | 주 1회 | 일요일 04:00 | 메타데이터 정리 |
| 파일 크기 모니터링 | 일 1회 | 매일 06:00 | Small File 감시 |
"""

    return guide


def main():
    parser = argparse.ArgumentParser(description="최종 구현 아티팩트 생성")
    parser.add_argument("--config", "-c", required=True, help="설계 구성 JSON 파일")
    parser.add_argument("--output-dir", "-o", default="./output", help="출력 디렉토리")
    parser.add_argument("--namespace", "-n", default="lakehouse", help="카탈로그 네임스페이스")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    out_dir = Path(args.output_dir)
    ns = args.namespace

    # 디렉토리 생성
    (out_dir / "ddl").mkdir(parents=True, exist_ok=True)
    (out_dir / "etl").mkdir(parents=True, exist_ok=True)
    (out_dir / "ops").mkdir(parents=True, exist_ok=True)
    (out_dir / "docs").mkdir(parents=True, exist_ok=True)

    all_tables = config.get("tables", []) + config.get("gold_tables", [])
    sources = config.get("sources", [])

    print(f"{'='*60}")
    print(f"🏗️  아티팩트 생성 시작 ({len(all_tables)} 테이블)")
    print(f"{'='*60}")

    # 1. DDL 생성
    ddl_all = []
    for tbl in all_tables:
        ddl = generate_iceberg_ddl(tbl, ns)
        ddl_path = out_dir / "ddl" / f"{tbl['name']}.sql"
        ddl_path.write_text(ddl, encoding="utf-8")
        ddl_all.append(ddl)
        print(f"  ✅ DDL: {ddl_path}")

    # 통합 DDL
    (out_dir / "ddl" / "all_tables.sql").write_text("\n\n".join(ddl_all), encoding="utf-8")

    # 2. ETL 스크립트 생성
    for tbl in all_tables:
        etl = generate_pyspark_etl(tbl, sources, ns)
        etl_path = out_dir / "etl" / f"etl_{tbl['name']}.py"
        etl_path.write_text(etl, encoding="utf-8")
        print(f"  ✅ ETL: {etl_path}")

    # 3. 운영 가이드 생성
    ops = generate_ops_guide(all_tables, ns)
    ops_path = out_dir / "ops" / "operations_guide.md"
    ops_path.write_text(ops, encoding="utf-8")
    print(f"  ✅ OPS: {ops_path}")

    # 4. 메타데이터 문서
    metadata = {
        "generated_at": datetime.now().isoformat(),
        "namespace": ns,
        "tables": [],
    }
    for tbl in all_tables:
        metadata["tables"].append({
            "name": tbl["name"],
            "layer": tbl.get("layer", "unknown"),
            "mode": tbl.get("mode", "COW"),
            "description": tbl.get("description", ""),
            "columns": [
                {
                    "name": c["name"],
                    "type": c.get("type", "STRING"),
                    "nullable": c.get("nullable", True),
                    "description": c.get("description", ""),
                }
                for c in tbl.get("columns", [])
            ],
            "partition_spec": tbl.get("partition_spec", []),
            "sort_order": tbl.get("sort_order", []),
            "retention_days": tbl.get("retention_days"),
        })

    meta_path = out_dir / "docs" / "table_metadata.json"
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  ✅ META: {meta_path}")

    print(f"\n{'='*60}")
    print(f"🎉 아티팩트 생성 완료: {out_dir}")
    print(f"   ddl/  — Iceberg DDL 문 ({len(all_tables) + 1} 파일)")
    print(f"   etl/  — PySpark ETL 스크립트 ({len(all_tables)} 파일)")
    print(f"   ops/  — 운영 가이드")
    print(f"   docs/ — 테이블 메타데이터")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
