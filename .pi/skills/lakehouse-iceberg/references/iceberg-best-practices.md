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

```sql
-- 만료된 스냅샷 제거 (보관 주기 설정)
CALL catalog.expire_snapshots(
  'lakehouse.table_name',
  TIMESTAMP '2026-01-01 00:00:00'
);

-- 롤백 (장애 시 복구)
CALL catalog.rollback_to_snapshot(
  'lakehouse.table_name',
  <snapshot_id>
);

-- 스냅샷 목록 확인
SELECT snapshot_id, committed_at, operation
FROM lakehouse.table_name.snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

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
