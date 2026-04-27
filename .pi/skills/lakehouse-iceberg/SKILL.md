---
name: lakehouse-iceberg
description: Apache Iceberg 물리 구현을 담당합니다. Phase 3a 추상 전략을 MOR/COW·파티션 transforms·Sort Order로 번역하고, Iceberg DDL·PySpark ETL·운영 스크립트·Spark 검증 스크립트를 생성합니다. lakehouse-design 스킬로 business_model.json 생성 후 사용합니다.
---

# Lakehouse Iceberg Skill

`business_model.json`을 입력으로 받아 **Iceberg 물리 구현**을 생성합니다.
모든 명령은 **프로젝트 루트**에서 실행합니다.

## 전제 조건

`business_model.json`이 존재해야 합니다.
(lakehouse-design 스킬 → Phase 2에서 생성)

---

## Phase 3b: 추상 전략 → Iceberg 물리 번역 규칙

| 추상 개념 | Iceberg 물리 개념 |
|---|---|
| write_pattern: frequent | MOR (Merge-on-Read) |
| write_pattern: occasional | MOR 또는 COW (데이터량 기준 결정) |
| write_pattern: rare | COW (Copy-on-Write) |
| read_pattern: range_scan + time | `days()` 또는 `months()` transform |
| read_pattern: point_lookup | `bucket(N, key)` transform |
| read_pattern: full_scan | 파티션 최소화, Sort Order 중심 |
| query_focus (저카디널리티) | identity 파티션 후보 |
| query_focus (고카디널리티) | Sort Order / Z-Order (파티션 금지) |

레이어별 기본 원칙:
- Silver 레이어 → **MOR** (빈번한 CDC/UPSERT)
- Gold 레이어 → **COW** (읽기 최적, 수정 빈도 낮음)

⚠️ 파티션 결정 주의:
- 카디널리티가 높은 컬럼(Serial, UUID 등)을 파티션으로 설정하면 Small File Problem 발생
- 반드시 사용자에게 카디널리티 수치를 보여주고 승인받을 것

---

## Phase 5: Spark 물리 검증 스크립트 생성

```bash
python scripts/generate_spark_validation.py \
  --config business_model.json \
  --output-dir output/validation
```

생성 파일:
- `output/validation/spark_validate_partitioning.py` → 파티션 transform 동작 확인
- `output/validation/spark_validate_mor_merge.py` → MOR 머지 정확성 확인
- `output/validation/spark_validate_checklist.md` → 실행 순서 가이드

⚠️ 이 스크립트는 실제 **Spark + Iceberg 환경**에서 실행해야 합니다.

검증 체크리스트 항목 (포맷별):
- `days(event_ts)` 파티션 transform 동작
- MOR 머지 충돌 없음
- Iceberg 카탈로그 연동
- 스냅샷 타임트래블 동작
- 대용량 성능 (실제 데이터 기준)

---

## Phase 6: 구현 아티팩트 생성

```bash
# Step 1: business_model.json → design_config.json 변환
python scripts/merge_configs.py \
  --input business_model.json \
  --output design_config.json

# Step 2: Iceberg DDL + PySpark ETL + 운영 스크립트 생성
python scripts/generate_artifacts.py \
  --config design_config.json \
  --output-dir ./output \
  --namespace <네임스페이스>

# Step 3: 비즈니스 용어 사전 생성
python scripts/generate_term_glossary.py \
  --input business_model.json \
  --output output/docs/term_glossary.md
```

생성 산출물:
```
output/
├── ddl/
│   ├── <table_name>.sql          → Iceberg CREATE/ALTER TABLE
│   └── all_tables.sql            → 전체 통합
├── sql/
│   ├── insert_<table>.sql        → Spark SQL INSERT (append 패턴)
│   ├── upsert_<table>.sql        → Spark SQL MERGE INTO (upsert 패턴)
│   └── aggregate_<table>.sql     → Spark SQL 집계 (gold 패턴)
├── etl/
│   └── run_etl.py                → 얇은 Python 래퍼
│                                    (SparkSession + watermark 관리 + SQL 파일 실행)
├── ops/
│   ├── compaction.sql            → 정기 Compaction
│   ├── snapshot_management.sql   → 스냅샷 만료 관리
│   └── z_order_optimization.sql  → Z-Order 최적화
├── validation/
│   ├── spark_validate_*.py       → Spark 물리 검증 스크립트
│   └── spark_validate_checklist.md
└── docs/
    ├── user_guide.md             → 엔진 호환성, 주의사항
    ├── term_glossary.md          → 비즈니스 용어 사전
    └── table_metadata.json       → 테이블·컬럼 메타데이터
```

Spark SQL 파라미터 치환 방식: `python.format()`
- `{last_watermark}` → watermark.json 에서 읽음
- `{target_date}`    → 전날 날짜 (`date.today() - timedelta(1)`)
- `{namespace}`     → 실행 시 주입

---

## 검증 인증서 상태 표시

산출물 상단에 자동으로 포함:

```
🟦 논리 검증 (DuckDB): PASSED | 2026-04-27
🟧 물리 검증 (Spark):  SKIPPED (사용자 임의 승인) | 2026-04-27
```

Spark 검증이 SKIPPED인 경우 user_guide.md에 미검증 항목 경고 자동 삽입.

---

## ALTER 모드 DDL

ALTER 모드에서는 `CREATE TABLE` 대신 `ALTER TABLE` 문법으로 생성:

```sql
-- 컬럼 추가
ALTER TABLE lakehouse.gold_production_quality
ADD COLUMN factory_id STRING;

-- 타입 확장 (안전)
ALTER TABLE lakehouse.silver_production_events
ALTER COLUMN total_count TYPE BIGINT;
```

파티션 변경은 별도 마이그레이션 전략 문서와 함께 제공.

---

## 참고 문서

- [Iceberg 베스트 프랙티스](references/iceberg-best-practices.md)
