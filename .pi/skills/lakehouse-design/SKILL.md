---
name: lakehouse-design
description: 데이터 레이크하우스 설계의 포맷 무관 단계를 실행합니다. 소스 데이터 프로파일링(CSV/Parquet/JSON), Mermaid 데이터 계보 다이어그램 생성, 스키마 기반 Mock 데이터 생성, DuckDB 샌드박스 논리 검증, 비즈니스 용어 사전 생성을 담당합니다. Iceberg/Delta/Hive 어떤 포맷에서도 동일하게 사용됩니다.
---

# Lakehouse Design Skill

소스 분석부터 논리 검증까지 **포맷 무관** 단계를 실행합니다.
모든 명령은 **프로젝트 루트**에서 실행합니다.

## Setup (최초 1회)

```bash
bash setup.sh
source .venv/bin/activate
```

---

## Phase 2: 소스 프로파일링

```bash
# 단일 소스
python scripts/profile_source.py \
  --input <파일경로> \
  --output business_model.json

# 대용량 (샘플링)
python scripts/profile_source.py \
  --input <파일경로> \
  --sample 10000 \
  --output business_model.json

# 소스 이름 지정
python scripts/profile_source.py \
  --input <파일경로> \
  --name mes_production_log \
  --output profile_report.json
```

지원 포맷: CSV, TSV, Parquet, JSON, JSONL

결과 해석 포인트:
- `partition_candidates` → 파티션 후보 컬럼 목록
- `sort_candidates` → Sort Order 후보 (카디널리티 높은 컬럼)
- `quality_warnings` → 데이터 품질 이슈
- `top_values` → 코드값 의미 파악용

---

## Phase 4: 시각화

```bash
# Mermaid 데이터 계보 다이어그램
python scripts/generate_mermaid.py \
  --config business_model.json \
  --output lineage.md

# Mock 데이터 미리보기 (전체 테이블)
python scripts/generate_mock_data.py \
  --schema business_model.json \
  --rows 10

# Mock 데이터 미리보기 (특정 테이블)
python scripts/generate_mock_data.py \
  --schema business_model.json \
  --rows 5 \
  --table gold_production_quality

# 비즈니스 용어 사전 생성
python scripts/generate_term_glossary.py \
  --input business_model.json \
  --output output/docs/term_glossary.md
```

---

## Phase 5: DuckDB 논리 검증

```bash
# 기본 실행
python scripts/sandbox_validate.py \
  --config business_model.json \
  --rows 1000

# 추가 SQL 파일 포함
python scripts/sandbox_validate.py \
  --config business_model.json \
  --sql-dir ./custom_queries/ \
  --output output/validation/duckdb_report.json
```

검증 항목:
- NOT NULL 제약 조건
- 중복 제거 로직 (dedup key 기준)
- KPI 집계 정확성
- 날짜 경계 처리 (심야 근무 등)
- JOIN 로직 정합성

결과:
- ✅ ALL PASSED → 🟦 논리 검증 인증서 발급
- ❌ FAILED → 에러 분석 후 설계 수정 필요

⚠️ DuckDB는 로컬 논리 검증 전용입니다.
Iceberg 파티션 transforms, MOR 머지, 카탈로그 연동은
Spark 환경에서 별도 검증이 필요합니다.

---

## 참고 문서

- [데이터 모델링 원칙](references/modeling-principles.md)
