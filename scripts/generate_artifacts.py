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


def generate_user_guide(tables: list, sources: list, config: dict, namespace: str = "lakehouse") -> str:
    """사용자 가이드 생성 — 엔진 호환성, DDL/SQL 차이, 주의사항 포함"""

    # 테이블 요약 정보 구성
    table_summary_rows = []
    for tbl in tables:
        cols = tbl.get("columns", [])
        parts = ", ".join(
            f"{p.get('transform', 'identity')}({p['column']})" for p in tbl.get("partition_spec", [])
        ) or "없음"
        table_summary_rows.append(
            f"| `{namespace}.{tbl['name']}` | {tbl.get('layer', '-').upper()} "
            f"| {tbl.get('mode', '-')} | {parts} "
            f"| {', '.join(tbl.get('sort_order', [])) or '없음'} "
            f"| {tbl.get('retention_days', '-')}일 | {len(cols)}개 |"
        )
    table_summary = "\n".join(table_summary_rows)

    # 소스 요약
    source_rows = []
    for src in sources:
        source_rows.append(
            f"| `{src['name']}` | {src.get('origin', '-')} "
            f"| {src.get('load_method', '-')} | {src.get('row_count', '-')} |"
        )
    source_summary = "\n".join(source_rows)

    # 컬럼 매핑 요약
    mapping_rows = []
    for m in config.get("column_mappings", []):
        mapping_rows.append(
            f"| `{m.get('source_table', '-')}` | `{m.get('source_column', '-')}` "
            f"| `{m.get('transform', 'direct')}` "
            f"| `{m.get('target_table', '-')}` | `{m.get('target_column', '-')}` | `{m.get('type', '-')}` |"
        )
    mapping_summary = "\n".join(mapping_rows) if mapping_rows else "| (매핑 정보 없음) | | | | | |"

    guide = f"""# 사용자 가이드 (User Guide)

생성일: {datetime.now().strftime("%Y-%m-%d")}
네임스페이스: `{namespace}`

---

## 1. 비즈니스 목적 및 KPI 정의
"""

    # 1) 비즈니스 KPI 섹션
    biz_context = config.get("business_context", "")
    if biz_context:
        guide += f"\n**비즈니스 목표**: {biz_context}\n"

    kpis = config.get("kpis", [])
    if kpis:
        guide += "\n### 1.1 핵심 KPI\n\n"
        guide += "| KPI | 산식 | 단위 | 설명 |\n"
        guide += "|---|---|---|---|\n"
        for kpi in kpis:
            guide += (
                f"| `{kpi.get('name', '-')}` "
                f"| `{kpi.get('formula', '-')}` "
                f"| {kpi.get('unit', '-')} "
                f"| {kpi.get('description', '-')} |\n"
            )

    dimensions = config.get("dimensions", [])
    if dimensions:
        guide += f"\n### 1.2 분석 차원 (Dimensions)\n\n"
        guide += ", ".join(f"`{d}`" for d in dimensions) + "\n"

    freshness = config.get("freshness", "")
    if freshness:
        guide += f"\n### 1.3 데이터 신선도 (Freshness)\n\n"
        guide += f"- 갱신 주기: **{freshness}**\n"

    guide += f"""
---

## 2. 데이터 리니지 (Data Lineage)

### 2.1 전체 데이터 흐름

```
"""

    # 2) 데이터 리니지 섹션 — config의 lineage와 sources로 ASCII 생성
    lineage = config.get("lineage", [])
    if lineage:
        # 소스 → 타겟 그룹핑
        silver_sources = [e for e in lineage if "silver" in e.get("to", "").lower()]
        gold_sources = [e for e in lineage if "gold" in e.get("to", "").lower()]

        # Bronze 노드
        for src in sources:
            origin = src.get("origin", "Source")
            guide += f"  [{src['name']}]  ({origin})\n"
        guide += "        │\n"

        # Silver 변환
        if silver_sources:
            transforms = [e.get("transform", "") for e in silver_sources if e.get("transform")]
            silver_tbl = silver_sources[0].get("to", "silver")
            guide += f"        ├── {' / '.join(transforms)}\n"
            guide += f"        ▼\n"
            silver_info = next((t for t in tables if t["name"] == silver_tbl), None)
            if silver_info:
                mode = silver_info.get("mode", "?")
                parts = ", ".join(
                    f"{p.get('transform', '')}({p['column']})" for p in silver_info.get("partition_spec", [])
                )
                guide += f"  [{silver_tbl}]  (Mode: {mode}, Partition: {parts})\n"
            else:
                guide += f"  [{silver_tbl}]\n"
            guide += "        │\n"

        # Gold 변환
        if gold_sources:
            transforms = [e.get("transform", "") for e in gold_sources if e.get("transform")]
            gold_tbl = gold_sources[0].get("to", "gold")
            guide += f"        ├── {' / '.join(transforms)}\n"
            guide += f"        ▼\n"
            gold_info = next((t for t in tables if t["name"] == gold_tbl), None)
            if not gold_info:
                gold_info = next((t for t in config.get("gold_tables", []) if t["name"] == gold_tbl), None)
            if gold_info:
                mode = gold_info.get("mode", "?")
                parts = ", ".join(
                    f"{p.get('transform', '')}({p['column']})" for p in gold_info.get("partition_spec", [])
                )
                cols_count = len(gold_info.get("columns", []))
                guide += f"  [{gold_tbl}]  (Mode: {mode}, Partition: {parts or '없음'}, {cols_count} columns)\n"
            else:
                guide += f"  [{gold_tbl}]\n"
    else:
        guide += "  Bronze (Raw Sources)  →  Silver (Cleaned & Joined)  →  Gold (Aggregated)\n"

    guide += f"""```

### 2.2 변환 상세

| From | To | Transform |
|---|---|---|
"""
    for edge in lineage:
        guide += f"| `{edge.get('from', '-')}` | `{edge.get('to', '-')}` | {edge.get('transform', 'direct')} |\n"

    guide += f"""
---

## 3. 아키텍처 개요

### 3.1 소스 데이터

| 소스 | 출처 | 적재 방식 | 행 수 |
|---|---|---|---|
{source_summary}
"""

    # 3) 소스 프로파일링 섹션
    profiles = config.get("source_profiles", [])
    if profiles:
        guide += "\n### 3.2 소스 프로파일링 요약\n\n"
        for prof in profiles:
            guide += f"#### {prof.get('source_name', 'Unknown')} ({prof.get('row_count', '?'):,}행, {prof.get('column_count', '?')}컬럼)\n\n"
            guide += "| 컬럼 | 타입 | Null 비율 | 카디널리티 | 의미 타입 | 파티션 제안 |\n"
            guide += "|---|---|---|---|---|---|\n"
            for col in prof.get("columns", []):
                part_sug = col.get("partition_suggestion", {})
                part_text = f"`{part_sug.get('transform', '')}` — {part_sug.get('reason', '')}" if part_sug else "-"
                guide += (
                    f"| `{col.get('name', '-')}` "
                    f"| {col.get('dtype', '-')} "
                    f"| {col.get('null_ratio', 0):.1%} "
                    f"| {col.get('cardinality_class', '-')} ({col.get('nunique', '-')}) "
                    f"| {col.get('semantic_type', '-')} "
                    f"| {part_text} |\n"
                )
            # 수치형 컬럼 통계
            numeric_cols = [c for c in prof.get("columns", []) if "stats" in c]
            if numeric_cols:
                guide += f"\n**수치형 컬럼 통계:**\n\n"
                guide += "| 컬럼 | min | max | mean | std | median |\n"
                guide += "|---|---|---|---|---|---|\n"
                for col in numeric_cols:
                    s = col["stats"]
                    guide += (
                        f"| `{col['name']}` "
                        f"| {s.get('min', '-')} "
                        f"| {s.get('max', '-')} "
                        f"| {s.get('mean', '-')} "
                        f"| {s.get('std', '-')} "
                        f"| {s.get('median', '-')} |\n"
                    )
            # 품질 경고
            warnings = prof.get("summary", {}).get("quality_warnings", [])
            if warnings:
                guide += "\n**⚠️ 품질 경고:**\n"
                for w in warnings:
                    guide += f"- {w}\n"
            guide += "\n"

    guide += f"""
### {"3.3" if profiles else "3.2"} 테이블 설계 요약

| 테이블 | 레이어 | Mode | 파티션 | Sort Order | 보관기간 | 컬럼 수 |
|---|---|---|---|---|---|---|
{table_summary}

---

## 4. 컬럼 매핑 (Source → Target)

| Source Table | Source Column | Transform | Target Table | Target Column | Type |
|---|---|---|---|---|---|
{mapping_summary}
"""

    # 4) 샌드박스 검증 결과 섹션
    validation = config.get("validation_result", {})
    if validation:
        summary = validation.get("summary", {})
        guide += f"""
---

## 5. 샌드박스 검증 결과

### 5.1 검증 요약

| 항목 | 결과 |
|---|---|
| 총 검사 수 | {summary.get('total_checks', '-')} |
| ✅ 통과 | {summary.get('passed', '-')} |
| ❌ 실패 | {summary.get('failed', '-')} |
| ⚠️ 경고 | {summary.get('warnings', '-')} |
| 최종 상태 | **{summary.get('status', '-')}** |

### 5.2 검증 상세

| 수준 | 내용 |
|---|---|
"""
        _icons = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "INFO": "ℹ️"}
        for detail in validation.get("details", []):
            icon = _icons.get(detail.get("level", ""), "•")
            guide += f"| {icon} {detail.get('level', '-')} | {detail.get('message', '-')} |\n"

        errors = validation.get("errors", [])
        if errors:
            guide += "\n### 5.3 에러 상세\n\n"
            for err in errors:
                guide += f"- **Phase**: {err.get('phase', '-')}, **Error**: {err.get('error', '-')}\n"

    # 엔진 호환성 섹션 번호 동적 조정
    next_section = 6 if validation else 5

    guide += f"""
---

## {next_section}. 쿼리 엔진 호환성 가이드

### {next_section}.1 지원 엔진 및 호환 수준

| 기능 | Apache Spark | Trino (Iceberg Connector) | DuckDB (로컬 검증) |
|---|---|---|---|
| `CREATE TABLE ... USING iceberg` | ✅ 네이티브 | ⚠️ 문법 변환 필요 | ❌ Iceberg 미지원 |
| `PARTITIONED BY (transform())` | ✅ Hidden Partition | ✅ Hidden Partition | ❌ |
| `TBLPROPERTIES (...)` | ✅ | ⚠️ `WITH (...)` 문법 | ❌ |
| `INSERT INTO ... SELECT` | ✅ | ✅ | ✅ |
| `MERGE INTO` (Upsert) | ✅ | ✅ (v2) | ❌ |
| `FIRST()` 집계 함수 | ✅ | ❌ → `arbitrary()` 사용 | ✅ |
| `writeTo().overwritePartitions()` | ✅ PySpark API | ❌ SQL만 지원 | ❌ |
| Compaction (`rewrite_data_files`) | ✅ | ✅ | ❌ |
| Snapshot Expire | ✅ | ✅ | ❌ |

### {next_section}.2 Spark → Trino DDL 변환

Spark 문법으로 생성된 DDL을 Trino에서 사용하려면 다음을 변환하세요:

**Spark DDL:**
```sql
CREATE TABLE IF NOT EXISTS {namespace}.example_table (
    col1  STRING NOT NULL,
    col2  TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(col2))
TBLPROPERTIES (
    'format-version' = '2',
    'write.distribution-mode' = 'hash'
);
```

**Trino DDL:**
```sql
CREATE TABLE IF NOT EXISTS {namespace}.example_table (
    col1  VARCHAR NOT NULL,
    col2  TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(col2)'],
    format_version = 2
);
```

**주요 타입 매핑:**

| Iceberg / Spark | Trino |
|---|---|
| `STRING` | `VARCHAR` |
| `TIMESTAMP` | `TIMESTAMP(6)` |
| `BIGINT` | `BIGINT` (동일) |
| `DOUBLE` | `DOUBLE` (동일) |
| `DATE` | `DATE` (동일) |

### {next_section}.3 SQL 함수 호환성

| 용도 | Spark SQL | Trino SQL |
|---|---|---|
| 첫 번째 값 | `FIRST(col)` | `arbitrary(col)` |
| 날짜 추출 | `CAST(ts AS DATE)` | `CAST(ts AS DATE)` ✅ 동일 |
| 시간 추출 | `HOUR(ts)` | `hour(ts)` ✅ 동일 |
| 바닥 함수 | `FLOOR(x)` | `floor(x)` ✅ 동일 |
| 조건부 집계 | `SUM(CASE WHEN...)` | `SUM(CASE WHEN...)` ✅ 동일 |
| 문자열 연결 | `CONCAT(a, b)` | `concat(a, b)` ✅ 동일 |

---

## {next_section + 1}. MOR / COW 전략 가이드

### {next_section + 1}.1 Merge-On-Read (MOR)

- **적용 대상**: Silver 레이어 (쓰기 빈번, 읽기는 ETL 위주)
- **장점**: 쓰기 성능 우수, CDC 대응 적합
- **단점**: 읽기 시 델타 파일 병합 오버헤드
- **필수 운영**: 주기적 Compaction으로 읽기 성능 유지

### {next_section + 1}.2 Copy-On-Write (COW)

- **적용 대상**: Gold 레이어 (읽기 빈번, 대시보드/리포트)
- **장점**: 읽기 성능 최적, 추가 병합 불필요
- **단점**: 업데이트 시 파일 전체 재작성
- **권장**: 배치 단위 파티션 덮어쓰기(`overwritePartitions`)

---

## {next_section + 2}. 파티셔닝 주의사항

### {next_section + 2}.1 Small File Problem 방지

| ❌ 위험 패턴 | ✅ 권장 패턴 |
|---|---|
| `PARTITIONED BY (serial_no)` — 카디널리티 너무 높음 | Sort Order 또는 Z-Order로 대체 |
| `PARTITIONED BY (hours(ts))` — 시간당 데이터 적을 때 | `days(ts)`로 상향 조정 |
| `PARTITIONED BY (col_a, col_b, col_c)` — 조합 폭발 | 최대 2개 파티션 키 권장 |

### {next_section + 2}.2 파티션 변경 (Schema Evolution)

Iceberg는 기존 데이터를 재작성하지 않고 파티션 전략을 변경할 수 있습니다:
```sql
-- Spark
ALTER TABLE {namespace}.table_name SET PARTITION SPEC (months(ts));

-- Trino
ALTER TABLE {namespace}.table_name SET PROPERTIES partitioning = ARRAY['month(ts)'];
```

---

## {next_section + 3}. 데이터 품질 체크리스트

운영 환경 적용 전 아래를 확인하세요:

- [ ] NOT NULL 제약이 걸린 컬럼에 실제 Null이 없는지 확인
- [ ] 조인 키(`production_id`, `product_model`) 정합성 확인
- [ ] 타임존 통일 (UTC vs KST) 확인
- [ ] 센서 데이터 지연(Late Arrival) 처리 방안 수립
- [ ] 목표 온도 범위 변경 시 마스터 테이블 업데이트 절차 확인

---

## {next_section + 4}. 보안 및 접근 제어

| 레이어 | 권장 권한 | 대상 |
|---|---|---|
| Bronze (Raw) | READ 전용 | ETL 서비스 계정만 |
| Silver | READ/WRITE | ETL 서비스 계정 |
| Gold | READ 전용 | 분석가, 대시보드, BI 도구 |

```sql
-- Spark/Trino 접근 제어 예시
GRANT SELECT ON {namespace}.gold_model_temp_quality TO ROLE analyst;
GRANT ALL ON {namespace}.silver_production_events TO ROLE etl_service;
```

---

## {next_section + 5}. 트러블슈팅

| 증상 | 원인 | 해결 |
|---|---|---|
| 쿼리 느림 (Gold) | 파일 수 과다 (Small File) | Compaction 실행 |
| 데이터 누락 | 센서 조인 실패 | `production_id` 정합성 확인 |
| OOM 에러 | Silver 전체 스캔 | 파티션 프루닝 확인 (`WHERE production_ts >= ...`) |
| 중복 데이터 | ETL 재실행 | `overwritePartitions` 사용 확인 |
| 스냅샷 메타 비대 | Expire 미실행 | `expire_snapshots` 정기 실행 |
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

    # 4. 사용자 가이드 생성 (엔진 호환성, DDL/SQL 차이, 주의사항)
    user_guide = generate_user_guide(all_tables, sources, config, ns)
    guide_path = out_dir / "docs" / "user_guide.md"
    guide_path.write_text(user_guide, encoding="utf-8")
    print(f"  ✅ GUIDE: {guide_path}")

    # 5. 메타데이터 문서
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
    print(f"   docs/ — 사용자 가이드 + 테이블 메타데이터")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
