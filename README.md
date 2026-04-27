# Lakehouse Modeling Agent

Apache Iceberg 기반 데이터 레이크하우스의 **하향식(Top-Down) 모델링**을 지원하는 pi 스킬 + 프롬프트 템플릿입니다.

## 핵심 원칙

- **Gold-First**: "어떤 데이터가 있는가?"가 아닌 "어떤 의사결정이 필요한가?"에서 출발
- **토론 우선**: 사용자와 충분한 문답을 거친 후에만 코드 생성
- **포맷 추상화**: 논리 모델(포맷 무관) ↔ 물리 구현(포맷별)을 분리
- **명시적 승인**: 모든 주요 결정에서 사용자 승인 게이트 존재

## 구조

```
lakehouse-modeling-agent/
├── .pi/
│   ├── prompts/
│   │   └── lh-start.md              # 오케스트레이터 (워크플로우 + 대화 규칙)
│   └── skills/
│       ├── lakehouse-design/
│       │   ├── SKILL.md             # 포맷 무관 실행 (Phase 1~5 DuckDB)
│       │   └── references/
│       │       └── modeling-principles.md
│       └── lakehouse-iceberg/
│           ├── SKILL.md             # Iceberg 물리 구현 (Phase 3b~6)
│           └── references/
│               └── iceberg-best-practices.md
│
├── scripts/                         # 자동화 스크립트 (포맷 무관)
│   ├── venv_guard.py                # venv 강제 실행 가드
│   ├── profile_source.py            # Phase 2: 소스 프로파일링
│   ├── generate_mermaid.py          # Phase 4: 계보 다이어그램
│   ├── generate_mock_data.py        # Phase 4: Mock 데이터
│   ├── generate_term_glossary.py    # Phase 4: 비즈니스 용어 사전 ★
│   ├── sandbox_validate.py          # Phase 5: DuckDB 논리 검증
│   ├── generate_spark_validation.py # Phase 5: Spark 검증 스크립트 생성 ★
│   ├── merge_configs.py             # Phase 6: business_model → design_config ★
│   └── generate_artifacts.py        # Phase 6: DDL + ETL + 운영 스크립트
│
├── setup.sh                         # venv 초기 설정
├── requirements.txt                 # 의존성
└── examples/
    └── design_config_example.json
```

★ 새로 추가된 스크립트

## 워크플로우 (2-Layer)

```
Prompt Template (.pi/prompts/lh-start.md)
  └── 오케스트레이터: 대화 규칙 + 승인 게이트 + 스킬 선언

       ┌─────────────────────────────────────────────────┐
       │           lakehouse-design 스킬                 │
       │  Phase 1: 비즈니스 파악 (대화)                  │
       │  Phase 2: 소스 프로파일링                       │
       │  Phase 3a: 추상 전략 합의 (대화)                │
       │  Phase 4: 시각화 + 용어 사전                    │
       │  Phase 5: DuckDB 논리 검증 → 🟦 인증서         │
       └──────────────── 포맷 선택 게이트 ───────────────┘
                              ↓
       ┌─────────────────────────────────────────────────┐
       │          lakehouse-iceberg 스킬                 │
       │  Phase 3b: MOR/COW + 파티션 transform 결정      │
       │  Phase 5: Spark 검증 스크립트 생성 → 🟧 체크리스트│
       │  Phase 6: DDL + ETL + 운영 스크립트 생성        │
       └─────────────────────────────────────────────────┘
```

## 시작 방법

### 1. 환경 설정 (최초 1회)

```bash
git clone <this-repo>
cd lakehouse-modeling-agent
bash setup.sh
source .venv/bin/activate
```

### 2. pi에서 시작

```
/lh-start
```

pi가 `business_model.json` 존재 여부를 자동 감지하여 CREATE / ALTER 모드로 진입합니다.

### 3. 스킬 직접 호출 (필요 시)

```
/skill:lakehouse-design     # 소스 프로파일링, DuckDB 검증
/skill:lakehouse-iceberg    # Iceberg 물리 구현, Spark 검증 스크립트
```

## 스크립트 직접 실행

```bash
# Phase 2: 소스 프로파일링
python scripts/profile_source.py --input data.csv --output business_model.json

# Phase 4: 시각화
python scripts/generate_mermaid.py --config business_model.json --output lineage.md
python scripts/generate_mock_data.py --schema business_model.json --rows 10
python scripts/generate_term_glossary.py --input business_model.json --output output/docs/term_glossary.md

# Phase 5: 검증
python scripts/sandbox_validate.py --config business_model.json
python scripts/generate_spark_validation.py --config business_model.json --output-dir output/validation

# Phase 6: 구현
python scripts/merge_configs.py --input business_model.json --output design_config.json
python scripts/generate_artifacts.py --config design_config.json --output-dir ./output
```

## 핵심 파일

| 파일 | 역할 | 생성 시점 |
|---|---|---|
| `business_model.json` | 논리 모델 (포맷 무관) | Phase 2 완료 후 |
| `design_config.json` | 물리 모델 (Iceberg 포함) | Phase 6 직전 |
| `output/docs/term_glossary.md` | 비즈니스 용어 사전 | Phase 4 |
| `output/validation/` | Spark 검증 스크립트 | Phase 5 |
| `output/ddl/` | Iceberg DDL | Phase 6 |
| `output/etl/` | PySpark ETL | Phase 6 |
| `output/ops/` | 운영 스크립트 | Phase 6 |

## 새 포맷 추가 방법

```
.pi/skills/lakehouse-delta/ 디렉토리 생성
├── SKILL.md         # Delta Lake 물리 번역 규칙
└── references/
    └── delta-best-practices.md
```

`lakehouse-design` 스킬과 모든 scripts/는 수정 없이 재사용됩니다.

## 의존성

```
pandas >= 1.5.0
pyarrow >= 10.0.0
duckdb >= 0.9.0
```

PySpark는 운영 환경 전용 (로컬 스크립트 실행에 불필요).
