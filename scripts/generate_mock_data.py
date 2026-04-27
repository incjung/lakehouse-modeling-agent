#!/usr/bin/env python3
"""
스키마 기반 가상(Mock) 데이터 생성 스크립트 (Phase 4)
Gold Table 스키마 정의를 입력받아 사실적인 미리보기 데이터를 생성합니다.
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── venv 강제 실행 가드 ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from venv_guard import ensure_venv
ensure_venv()
# ──────────────────────────────────────────────────────────

try:
    import pandas as pd
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "-q"])
    import pandas as pd


# 타입별 생성 전략
GENERATORS = {
    "DATE": lambda col, i: (datetime(2024, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d"),
    "TIMESTAMP": lambda col, i: (
        datetime(2024, 1, 1, 8, 0, 0) + timedelta(hours=i * 2, minutes=random.randint(0, 59))
    ).isoformat(),
    "STRING": None,  # 이름 기반으로 별도 처리
    "VARCHAR": None,
    "INT": lambda col, i: random.randint(1, 10000),
    "INTEGER": lambda col, i: random.randint(1, 10000),
    "BIGINT": lambda col, i: random.randint(100000, 9999999),
    "FLOAT": lambda col, i: round(random.uniform(0.0, 100.0), 2),
    "DOUBLE": lambda col, i: round(random.uniform(0.0, 1000.0), 4),
    "DECIMAL": lambda col, i: round(random.uniform(0.0, 100.0), 2),
    "BOOLEAN": lambda col, i: random.choice([True, False]),
}

# 컬럼 이름 패턴 기반 현실적 값 생성
NAME_PATTERNS = {
    "line": lambda i: f"LINE-{random.choice(['A', 'B', 'C', 'D'])}{random.randint(1, 5):02d}",
    "product": lambda i: f"PRD-{random.choice(['X', 'Y', 'Z'])}{random.randint(100, 999)}",
    "model": lambda i: random.choice(["Model-A1", "Model-B2", "Model-C3", "Model-D4"]),
    "shift": lambda i: random.choice(["DAY", "NIGHT", "SWING"]),
    "status": lambda i: random.choice(["PASS", "FAIL", "PENDING"]),
    "rate": lambda i: round(random.uniform(85.0, 99.9), 2),
    "count": lambda i: random.randint(10, 5000),
    "ratio": lambda i: round(random.uniform(0.0, 1.0), 4),
    "defect": lambda i: random.randint(0, 50),
    "yield": lambda i: round(random.uniform(90.0, 99.99), 2),
    "temperature": lambda i: round(random.uniform(20.0, 80.0), 1),
    "pressure": lambda i: round(random.uniform(1.0, 10.0), 2),
    "speed": lambda i: round(random.uniform(100.0, 3000.0), 1),
    "operator": lambda i: f"OP-{random.randint(1001, 1050)}",
    "batch": lambda i: f"BATCH-{random.randint(20240101, 20241231)}",
}


def generate_value(column: dict, row_index: int):
    """컬럼 정의에 따라 적절한 mock 값을 생성"""
    col_name = column["name"].lower()
    col_type = column.get("type", "STRING").upper()

    # enum이 정의된 경우 그 중에서 선택
    if "enum" in column:
        return random.choice(column["enum"])

    # 타입 우선: 숫자/날짜/불린 타입은 타입 기반 생성을 먼저 적용
    if col_type in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "INT", "INTEGER",
                     "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "BOOLEAN"):
        generator = GENERATORS.get(col_type)
        if generator:
            return generator(column, row_index)

    # 문자열 타입: 이름 패턴 매칭
    for pattern, gen in NAME_PATTERNS.items():
        if pattern in col_name:
            return gen(row_index)

    # ID 컬럼 (문자열)
    if col_name.endswith("_id") or col_name.endswith("_key"):
        prefix = col_name.replace("_id", "").replace("_key", "").upper()[:4]
        return f"{prefix}-{random.randint(1, 200):04d}"

    # 타입 기반 폴백
    generator = GENERATORS.get(col_type)
    if generator:
        return generator(column, row_index)

    # 기본값: 문자열
    return f"{column['name']}_val_{row_index}"


def generate_mock_dataframe(schema: dict, num_rows: int) -> pd.DataFrame:
    """스키마 정의에서 mock DataFrame 생성"""
    columns = schema.get("columns", [])
    data = {}

    for col in columns:
        nullable = col.get("nullable", True)
        values = []
        for i in range(num_rows):
            if nullable and random.random() < 0.02:  # 2% Null
                values.append(None)
            else:
                values.append(generate_value(col, i))
        data[col["name"]] = values

    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="스키마 기반 Mock 데이터 생성")
    parser.add_argument("--schema", "-s", required=True, help="Gold Table 스키마 JSON 파일")
    parser.add_argument("--rows", "-r", type=int, default=10, help="생성할 행 수 (기본: 10)")
    parser.add_argument("--output", "-o", default=None, help="출력 파일 (csv/json, 미지정 시 터미널 출력)")
    parser.add_argument("--table", "-t", default=None, help="특정 테이블만 생성 (이름 지정)")
    args = parser.parse_args()

    schema_data = json.loads(Path(args.schema).read_text(encoding="utf-8"))

    # gold_tables 배열 또는 단일 테이블 지원
    tables = schema_data.get("gold_tables", [schema_data])
    if args.table:
        tables = [t for t in tables if t.get("name") == args.table]
        if not tables:
            print(f"❌ 테이블 '{args.table}'을 찾을 수 없습니다.")
            sys.exit(1)

    for table_schema in tables:
        table_name = table_schema.get("name", "unknown_table")
        print(f"\n{'='*60}")
        print(f"📊 Mock Data Preview: {table_name} ({args.rows}행)")
        print(f"{'='*60}")

        df = generate_mock_dataframe(table_schema, args.rows)

        if args.output:
            out_path = Path(args.output)
            if out_path.suffix == ".csv":
                df.to_csv(out_path, index=False)
            elif out_path.suffix == ".json":
                df.to_json(out_path, orient="records", force_ascii=False, indent=2)
            else:
                df.to_csv(out_path, index=False)
            print(f"✅ 저장 완료: {out_path}")
        else:
            print(df.to_string(index=False))

        # 컬럼 요약
        print(f"\n📋 컬럼 요약:")
        for col in table_schema.get("columns", []):
            desc = col.get("description", "")
            null_mark = "⭕ Nullable" if col.get("nullable", True) else "🔴 NOT NULL"
            print(f"   {col['name']:30s} {col.get('type', '?'):12s} {null_mark}  {desc}")


if __name__ == "__main__":
    main()
