# 데이터 모델링 원칙 (Format-Agnostic)

## Gold-First 설계 원칙

항상 최종 산출물(Gold)에서 역방향으로 설계한다.

```
❌ Bottom-Up (잘못된 방식)
"어떤 데이터가 있는가?" → 있는 데이터로 무언가 만든다

✅ Top-Down (올바른 방식)
"어떤 의사결정이 필요한가?" → 필요한 데이터를 역으로 설계한다
```

## 비즈니스 용어 매핑 원칙

### 매핑 확인 없이 진행 가능한 경우
- 컬럼명이 비즈니스 용어와 동일할 때 (예: `production_date` → "생산일자")
- 타입이 명확하고 후보가 하나일 때 (예: `is_defect BOOLEAN`)
- 값이 자명할 때 (예: `is_active = true/false`)

### 반드시 사용자 확인이 필요한 경우
- 같은 개념의 후보 컬럼이 여러 개 (예: `status`, `quality_code`, `is_rejected`)
- 코드값 컬럼 (예: `shift_code = 1/2/3` → 주간/야간/심야?)
- 파생 컬럼 계산식 (예: 불량률 = ? / ?)
- 비즈니스 용어가 데이터에 존재하지 않을 때

### 매핑 테이블 구조
```json
{
  "business_term": "불량률",
  "source_table": null,
  "source_column": null,
  "sql_expression": "COUNT(CASE WHEN status='FAIL' THEN 1 END) / COUNT(*) * 100",
  "is_derived": true,
  "confirmed_by_user": true,
  "confirmed_at": "2026-04-27T14:23:00"
}
```

## 멀티 소스 처리 원칙

### 미사용 소스 발견 시 처리 흐름
1. 자동 무시 ❌ | 자동 Silver 적재 ❌
2. 사용자에게 의도 파악 질문
3. 불필요 → 제외 후 문서에 기록
4. 미래 확장 → Silver 선적재 or 제외 (사용자 선택)
5. 계산식 기반으로 누락 소스 역탐지 (가장 중요)

### JOIN 관계 확인
- JOIN 키 불일치 행 수를 보여주고 처리 방침 결정
- LEFT JOIN으로 자동 처리 ❌ → 반드시 사용자 확인

## 엣지 케이스 선제 발굴 체크리스트

```
□ 날짜 경계: 심야 근무(22-06)는 어느 날짜 기준?
□ 타임존: UTC/KST 혼용 가능성?
□ 지연 데이터: 나중에 들어오는 소스가 있는가?
□ 중복 이벤트: 중복 제거 기준 키가 있는가?
□ NULL 의미: NULL = 미입력인가, 해당없음인가?
□ 코드 변경: 과거에 코드값이 바뀐 적 있는가?
□ 미래 확장: 다른 공장/시스템 데이터 추가 예정?
```

## 소스 프로파일링 해석 가이드

| 카디널리티 | 의미 | 파티션 전략 |
|---|---|---|
| unique (>95%) | Serial, UUID 류 | Sort Order 권장, 파티션 금지 |
| high (50~95%) | ID 류 | bucket() 파티션 검토 |
| medium (5~50%) | 범주형 | identity 파티션 검토 |
| low (<5%) | 상태값, 코드값 | identity 파티션 가능 |

## DuckDB 논리 검증 한계

DuckDB는 **비즈니스 로직만** 검증합니다:
- ✅ SQL 집계·JOIN·NULL 처리
- ✅ NOT NULL 제약
- ✅ 중복 제거 로직
- ❌ Iceberg 파티션 transforms
- ❌ MOR 머지 동작
- ❌ 카탈로그 연동
- ❌ 대용량 성능

## Medallion Architecture 레이어 역할

```
Bronze (Raw)
└── 원본 데이터 그대로 보존
    변환 없음, 삭제 없음

Silver (Refined)
└── 중복 제거, 타입 정규화
    품질 검증, 마스터 데이터 JOIN
    이력 보관 (CDC)

Gold (Aggregated)
└── 비즈니스 KPI 집계
    읽기 최적화
    다운스트림 직접 소비
```
