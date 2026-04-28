# Apache Iceberg 베스트 프랙티스

## MOR vs COW 결정 가이드

| 기준 | MOR (Merge-on-Read) | COW (Copy-on-Write) |
|---|---|---|
| 쓰기 빈도 | 높음 (CDC, 실시간) | 낮음 (일배치) |
| 읽기 빈도 | 낮음~중간 | 높음 |
| UPDATE/DELETE | 빈번 | 거의 없음 |
| 읽기 성능 | 상대적으로 낮음 | 높음 |
| 쓰기 성능 | 높음 | 낮음 (파일 재작성) |
| 적합한 레이어 | Silver | Gold |

```sql
-- MOR 설정
TBLPROPERTIES (
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read'
)

-- COW 설정
TBLPROPERTIES (
  'write.distribution-mode' = 'none'
)
```

## 증분 쿼리 구현 패턴 (Incremental Query Patterns)
Iceberg의 증분 읽기는 전체 데이터를 스캔하지 않고 스냅샷 간의 차이($\Delta$)만 읽어 처리 효율을 극대화합니다. 실버(Silver) 및 골드(Gold) 레이어 적재 시 다음 패턴을 적용합니다.
### 증분 처리 시 고려사항 (Critical Considerations)
- Idempotency (멱등성): 증분 쿼리는 여러 번 실행해도 결과가 같아야 합니다. MERGE INTO 사용 시 target.event_ts < source.event_ts 조건을 추가하여 뒤늦게 도착한 과거 데이터가 최신 데이터를 덮어쓰지 않도록 방어해야 합니다.
- Snapshot Validity: start-snapshot-id가 expire_snapshots에 의해 이미 삭제된 경우 쿼리가 실패합니다. 반드시 증분 배치 주기 < 스냅샷 보존 기간이 유지되도록 설정하십시오.
- Schema Evolution: Iceberg는 스키마 변경을 완벽히 지원하지만, 증분 읽기 도중 원천 테이블의 컬럼이 삭제되거나 타입이 변경된 경우 타겟 테이블과의 매핑 로직을 사전에 점검해야 합니다.
### Spark DataFrame API 패턴
데이터 엔지니어링 파이프라인에서 가장 권장되는 방식입니다. start-snapshot-id를 명시하여 지난 배치 이후의 데이터만 추출합니다.

```python
# 1. 체크포인트 테이블에서 마지막 처리된 스냅샷 ID 조회
last_id = spark.sql("SELECT last_snapshot_id FROM audit.checkpoints WHERE table='bronze_logs'").collect()[0][0]

# 2. 증분 데이터 읽기 (last_id 이후부터 현재 최신 스냅샷까지)
incremental_df = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", last_id) \
    .load("prod.db.bronze_logs")

# 3. 읽어온 데이터가 있을 경우에만 MERGE 실행
if not incremental_df.isEmpty():
    incremental_df.createOrReplaceTempView("inc_source")
    
    # MERGE INTO 실행 (Deduplication 로직 포함)
    spark.sql("""
        MERGE INTO prod.db.silver_logs AS target
        USING (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY event_ts DESC) as rn
                FROM inc_source
            ) WHERE rn = 1
        ) AS source
        ON target.id = source.id
        WHEN MATCHED AND target.event_ts < source.event_ts THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    # 4. 체크포인트 업데이트 (현재 테이블의 최신 스냅샷 ID로)
    new_last_id = spark.sql("SELECT snapshot_id FROM prod.db.bronze_logs.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
    spark.sql(f"UPDATE audit.checkpoints SET last_snapshot_id = '{new_last_id}' WHERE table='bronze_logs'")
```

### Spark SQL 함수 패턴 (Table-Valued Function)
Ad-hoc 분석이나 단순 SQL 기반 ETL에서 유용하게 사용되는 방식입니다.
```sql
-- 두 스냅샷 사이의 변경분 조회
SELECT * FROM system.incremental_scan('prod.db.bronze_logs', 'from_snapshot_id', 'to_snapshot_id');

-- 실무 활용 예시 (지난 1시간 내 추가된 데이터 확인)
SELECT * FROM system.incremental_scan(
    'prod.db.bronze_logs', 
    (SELECT snapshot_id FROM prod.db.bronze_logs.snapshots WHERE committed_at < (NOW() - INTERVAL 1 HOUR) ORDER BY committed_at DESC LIMIT 1)
);
```



## 파티션 Transform 선택 가이드

| Transform | 사용 케이스 | 예시 |
|---|---|---|
| `days(ts)` | 일별 쿼리 패턴 | 이벤트 로그, CDC |
| `months(dt)` | 월별 리포팅 패턴 | KPI 집계, 정산 |
| `years(dt)` | 연간 아카이브 | 장기 보관 데이터 |
| `hours(ts)` | 시간별 실시간 | IoT, 로그 |
| `bucket(N, col)` | 포인트 조회 최적화 | 사용자 ID, 주문 ID |
| `truncate(N, col)` | 문자열 접두사 파티션 | 지역 코드, 카테고리 |

### ⚠️ Small File Problem 경고

카디널리티가 높은 컬럼을 파티션으로 설정하면 파일 수가 폭발합니다:
```
Serial Number (고유값) → 파티션으로 설정하면
  파티션 수 = 전체 행 수 → 수백만 개의 작은 파일
  → 메타데이터 오버헤드, 쿼리 성능 저하

해결: Sort Order 또는 bucket() 사용
```

## Sort Order vs Z-Order

```sql
-- Sort Order: 단일/다중 컬럼 정렬
ALTER TABLE table WRITE ORDERED BY (line_id, event_ts);
-- 적합: 컬럼 1~2개, 순차 범위 쿼리

-- Z-Order: 다차원 클러스터링
CALL catalog.rewrite_data_files(
  'table',
  strategy => 'sort',
  sort_order => 'zorder(line_id, product_id)'
);
-- 적합: 여러 컬럼을 동시에 필터링하는 쿼리
```

## 파티션 진화 (Partition Evolution)

Iceberg의 핵심 강점: 기존 데이터를 변경하지 않고 파티션 스펙 변경 가능

```sql
-- 기존: days(event_ts)로 파티션된 데이터 유지
-- 신규: hours(event_ts)로 새 데이터 적재
ALTER TABLE table
ADD PARTITION FIELD hours(event_ts);

-- 주의: 쿼리 시 두 파티션 스펙이 공존
-- 메타데이터 확인:
SELECT * FROM table.partitions;
```

## 스냅샷 관리

### 스냅샷 목록 확인

```sql
SELECT snapshot_id, committed_at, operation
FROM lakehouse.table_name.snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

### 패턴 1 — 특정 시간 이전 삭제 (권장)

현재 시점으로부터 특정 시간보다 오래된 스냅샷을 정리합니다.
`retain_last` 는 **필수 안전장치** — 없으면 타임트래블 포인트가 전부 사라질 수 있습니다.

```sql
CALL catalog.system.expire_snapshots(
    table        => 'lakehouse.table_name',
    older_than   => TIMESTAMP '2026-04-01 00:00:00', -- 이 시간 이전 것은 삭제
    retain_last  => 100                              -- 하지만 최신 100개는 무조건 보관
);
```

### 패턴 2 — 개수 기준 보관 (시간 무관)

날짜와 관계없이 최근 N개의 스냅샷만 유지합니다.
배치가 불규칙하거나 보관 기간보다 보관 개수가 더 중요한 경우에 적합합니다.

```sql
CALL catalog.system.expire_snapshots(
    table        => 'lakehouse.table_name',
    older_than   => now(),   -- 현재 시점 이전 전부 대상
    retain_last  => 30       -- 최신 30개만 보관
);
```

### 패턴 3 — 롤백 (장애 시 복구)

```sql
-- 1. 롤백 전 스냅샷 목록 확인
SELECT snapshot_id, committed_at, operation
FROM lakehouse.table_name.snapshots
ORDER BY committed_at DESC LIMIT 10;

-- 2. 특정 스냅샷으로 롤백
CALL catalog.rollback_to_snapshot(
    'lakehouse.table_name',
    <snapshot_id>
);
```

### 레이어별 권장 설정

| 레이어 | older_than | retain_last | 실행 주기 | 근거 |
|---|---|---|---|---|
| **Silver** (MOR) | 30일 | 100 | 주 1회 | 잦은 UPSERT → 스냅샷 빠르게 누적 |
| **Gold** (COW) | 90일 | 50 | 월 1회 | 쓰기 빈도 낮음, 타임트래블 길게 보존 |

> ⚠️ `retain_last` 를 생략하면 `older_than` 기준으로 **모든 스냅샷이 삭제될 수 있습니다.**
> 반드시 두 파라미터를 함께 사용하세요.

## Compaction 전략

```sql
-- 데이터 파일 병합 (Small File 해결)
CALL catalog.rewrite_data_files('lakehouse.silver_production_events');

-- 파티션 지정 Compaction
CALL catalog.rewrite_data_files(
  'lakehouse.silver_production_events',
  filter => "event_date >= '2026-04-01'"
);

-- 매니페스트 최적화
CALL catalog.rewrite_manifests('lakehouse.silver_production_events');
```

권장 Compaction 주기:
- Silver (MOR) → 주 1회 이상
- Gold (COW) → 월 1회 또는 대규모 적재 후

## ALTER 모드 안전 절차

```sql
-- 1. 변경 전 스냅샷 ID 기록
SELECT snapshot_id FROM table.snapshots
ORDER BY committed_at DESC LIMIT 1;

-- 2. 안전한 변경 (컬럼 추가)
ALTER TABLE table ADD COLUMN new_col STRING;

-- 3. 문제 발생 시 롤백
CALL catalog.rollback_to_snapshot('table', <snapshot_id>);
```

## Spark 설정 템플릿

```python
spark = (
    SparkSession.builder
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hive")  # 또는 "hadoop", "rest"
    .config("spark.sql.catalog.lakehouse.uri", "<metastore_uri>")
    .getOrCreate()
)
```

## Spark vs Trino DDL 호환성 주의

| 기능 | Spark | Trino |
|---|---|---|
| CREATE TABLE | `USING iceberg` | `WITH (format='PARQUET')` |
| MERGE INTO | 지원 | 지원 (문법 일부 다름) |
| 파티션 Transform | `PARTITIONED BY (days(ts))` | `PARTITIONED BY (day(ts))` |
| 카탈로그 | `catalog.db.table` | `catalog.schema.table` |
