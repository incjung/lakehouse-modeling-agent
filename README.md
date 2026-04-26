# Lakehouse Modeling Agent

Apache Iceberg 기반 데이터 레이크하우스의 하향식(Top-Down) 모델링을 지원하는 Claude Code 스킬입니다.

## 구조

```
lakehouse-modeling-agent/
├── CLAUDE.md                        # 스킬 정의 (Claude Code가 읽는 파일)
├── scripts/
│   ├── profile_source.py            # Phase 2: 소스 데이터 프로파일링
│   ├── generate_mermaid.py          # Phase 4: Mermaid 계보 다이어그램 생성
│   ├── generate_mock_data.py        # Phase 4: Mock 데이터 미리보기 생성
│   ├── sandbox_validate.py          # Phase 5: DuckDB 샌드박스 검증
│   └── generate_artifacts.py        # Phase 6: DDL/ETL/운영가이드 생성
├── examples/
│   └── design_config_example.json   # 설계 구성 예시
└── README.md
```

## 워크플로우

| Phase | 이름 | 주요 자동화 | 스크립트 |
|---|---|---|---|
| 1 | Discovery | 대화형 (KPI/Gold 정의) | — |
| 2 | Source Profiling | 카디널리티/Null/분포 분석 | `profile_source.py` |
| 3 | Strategy | 대화형 (MOR/COW, 파티션 승인) | — |
| 4 | Visualization | 다이어그램 + Mock 데이터 | `generate_mermaid.py`, `generate_mock_data.py` |
| 5 | Validation | DuckDB 샌드박스 테스트 | `sandbox_validate.py` |
| 6 | Implementation | DDL/ETL/운영가이드 생성 | `generate_artifacts.py` |

## 스크립트 사용법

### 소스 프로파일링 (Phase 2)
```bash
python scripts/profile_source.py --input data.csv --output profile.json
python scripts/profile_source.py --input data.parquet --sample 10000  # 대용량 샘플링
```

### Mermaid 다이어그램 (Phase 4)
```bash
python scripts/generate_mermaid.py --config design_config.json --output lineage.md
```

### Mock 데이터 (Phase 4)
```bash
python scripts/generate_mock_data.py --schema design_config.json --rows 10
python scripts/generate_mock_data.py --schema design_config.json --rows 5 --table gold_production_quality
```

### 샌드박스 검증 (Phase 5)
```bash
python scripts/sandbox_validate.py --config design_config.json --rows 100 --output report.json
python scripts/sandbox_validate.py --config design_config.json --sql-dir ./custom_queries/
```

### 아티팩트 생성 (Phase 6)
```bash
python scripts/generate_artifacts.py --config design_config.json --output-dir ./output --namespace my_lakehouse
```

## 의존성

```
pandas
pyarrow
duckdb
```

## Claude Code 스킬로 등록

이 디렉토리를 Claude Code의 프로젝트 루트에 포함하면, `CLAUDE.md`가 자동으로 스킬 지침으로 인식됩니다.
