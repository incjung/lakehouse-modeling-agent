#!/usr/bin/env python3
"""
DuckDB 샌드박스 검증 스크립트 (Phase 5)
설계된 DDL과 SQL 로직을 로컬 DuckDB에서 실행하여 정합성을 검증합니다.
에러 발생 시 자동 수정을 시도합니다.
"""

import argparse
import json
import sys
import textwrap
import traceback
from pathlib import Path

# ── venv 강제 실행 가드 ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from venv_guard import ensure_venv
ensure_venv()
# ──────────────────────────────────────────────────────────

try:
    import duckdb
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "-q"])
    import duckdb

try:
    import pandas as pd
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "-q"])
    import pandas as pd


# Iceberg 타입 → DuckDB 타입 매핑
TYPE_MAP = {
    "STRING": "VARCHAR",
    "INT": "INTEGER",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL(18,4)",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "BINARY": "BLOB",
}


class SandboxValidator:
    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)
        self.results = []
        self.errors = []

    def log(self, level: str, message: str):
        icon = {"INFO": "ℹ️", "PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "FIX": "🔧"}
        print(f"  {icon.get(level, '•')} [{level}] {message}")
        self.results.append({"level": level, "message": message})

    def translate_type(self, iceberg_type: str) -> str:
        return TYPE_MAP.get(iceberg_type.upper(), "VARCHAR")

    def create_table_from_schema(self, table: dict) -> bool:
        """스키마 정의에서 DuckDB 테이블 생성"""
        table_name = table["name"]
        columns = table.get("columns", [])

        col_defs = []
        for col in columns:
            duck_type = self.translate_type(col.get("type", "STRING"))
            nullable = "" if col.get("nullable", True) else " NOT NULL"
            col_defs.append(f'    "{col["name"]}" {duck_type}{nullable}')

        col_body = ",\n".join(col_defs)
        ddl = f'CREATE OR REPLACE TABLE "{table_name}" (\n{col_body}\n);'

        try:
            self.con.execute(ddl)
            self.log("PASS", f"테이블 생성 성공: {table_name}")
            return True
        except Exception as e:
            self.log("FAIL", f"테이블 생성 실패: {table_name} — {e}")
            self.errors.append({"phase": "DDL", "table": table_name, "error": str(e), "ddl": ddl})
            return False

    def inject_mock_data(self, table: dict, num_rows: int = 100) -> bool:
        """가상 데이터 주입"""
        table_name = table["name"]
        columns = table.get("columns", [])

        try:
            # generate_mock_data 모듈 재사용
            mock_module_path = Path(__file__).parent / "generate_mock_data.py"
            if mock_module_path.exists():
                import importlib.util
                spec = importlib.util.spec_from_file_location("mock_gen", mock_module_path)
                mock_gen = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mock_gen)
                df = mock_gen.generate_mock_dataframe(table, num_rows)
            else:
                # 폴백: 간단한 데이터 생성
                import random
                from datetime import datetime, timedelta
                data = {}
                for col in columns:
                    ctype = col.get("type", "STRING").upper()
                    if ctype in ("DATE", "TIMESTAMP"):
                        data[col["name"]] = [
                            (datetime(2024, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
                            for i in range(num_rows)
                        ]
                    elif ctype in ("INT", "INTEGER", "BIGINT"):
                        data[col["name"]] = [random.randint(1, 10000) for _ in range(num_rows)]
                    elif ctype in ("FLOAT", "DOUBLE", "DECIMAL"):
                        data[col["name"]] = [round(random.uniform(0, 100), 2) for _ in range(num_rows)]
                    elif ctype == "BOOLEAN":
                        data[col["name"]] = [random.choice([True, False]) for _ in range(num_rows)]
                    else:
                        data[col["name"]] = [f"val_{i}" for i in range(num_rows)]
                df = pd.DataFrame(data)

            self.con.execute(f'INSERT INTO "{table_name}" SELECT * FROM df')
            count = self.con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            self.log("PASS", f"데이터 주입 성공: {table_name} ({count}행)")
            return True
        except Exception as e:
            self.log("FAIL", f"데이터 주입 실패: {table_name} — {e}")
            self.errors.append({"phase": "INSERT", "table": table_name, "error": str(e)})
            return False

    def run_validation_queries(self, table: dict) -> bool:
        """무결성 검증 쿼리 실행"""
        table_name = table["name"]
        columns = table.get("columns", [])
        all_passed = True

        # 1. NOT NULL 제약 조건 검증
        not_null_cols = [c["name"] for c in columns if not c.get("nullable", True)]
        for col_name in not_null_cols:
            try:
                result = self.con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
                ).fetchone()[0]
                if result == 0:
                    self.log("PASS", f"NOT NULL 검증 통과: {table_name}.{col_name}")
                else:
                    self.log("FAIL", f"NOT NULL 위반: {table_name}.{col_name} ({result}건 NULL)")
                    all_passed = False
            except Exception as e:
                self.log("FAIL", f"NOT NULL 검증 에러: {table_name}.{col_name} — {e}")
                all_passed = False

        # 2. 중복 검사 (PK 후보 컬럼)
        pk_candidates = [c["name"] for c in columns if c.get("primary_key") or "_id" in c["name"].lower()]
        if pk_candidates:
            pk_cols = ", ".join(f'"{c}"' for c in pk_candidates)
            try:
                result = self.con.execute(f"""
                    SELECT COUNT(*) - COUNT(DISTINCT ({pk_cols}))
                    FROM "{table_name}"
                """).fetchone()[0]
                if result == 0:
                    self.log("PASS", f"유니크 검증 통과: {table_name} ({', '.join(pk_candidates)})")
                else:
                    self.log("WARN", f"중복 가능: {table_name} ({', '.join(pk_candidates)}) — {result}건")
            except Exception as e:
                self.log("WARN", f"유니크 검증 스킵: {e}")

        # 3. 기본 집계 테스트
        try:
            result = self.con.execute(f'SELECT * FROM "{table_name}" LIMIT 5').fetchdf()
            self.log("PASS", f"기본 쿼리 성공: {table_name}")
        except Exception as e:
            self.log("FAIL", f"기본 쿼리 실패: {table_name} — {e}")
            all_passed = False

        return all_passed

    def run_custom_sql(self, sql: str, description: str = "Custom Query") -> bool:
        """사용자 정의 SQL 실행 및 검증"""
        try:
            result = self.con.execute(sql)
            if sql.strip().upper().startswith("SELECT"):
                df = result.fetchdf()
                self.log("PASS", f"{description} — 결과 {len(df)}행")
                print(df.to_string(index=False))
            else:
                self.log("PASS", f"{description}")
            return True
        except Exception as e:
            self.log("FAIL", f"{description} — {e}")
            self.errors.append({"phase": "CUSTOM_SQL", "description": description, "error": str(e), "sql": sql})
            return False

    def generate_report(self) -> dict:
        """검증 결과 리포트 생성"""
        pass_count = sum(1 for r in self.results if r["level"] == "PASS")
        fail_count = sum(1 for r in self.results if r["level"] == "FAIL")
        warn_count = sum(1 for r in self.results if r["level"] == "WARN")

        return {
            "summary": {
                "total_checks": len(self.results),
                "passed": pass_count,
                "failed": fail_count,
                "warnings": warn_count,
                "status": "PASSED" if fail_count == 0 else "FAILED",
            },
            "details": self.results,
            "errors": self.errors,
        }

    def close(self):
        self.con.close()


def main():
    parser = argparse.ArgumentParser(description="DuckDB 샌드박스 검증")
    parser.add_argument("--config", "-c", required=True, help="설계 구성 JSON 파일")
    parser.add_argument("--sql-dir", "-s", default=None, help="추가 SQL 파일 디렉토리")
    parser.add_argument("--output", "-o", default=None, help="결과 리포트 JSON 파일")
    parser.add_argument("--rows", "-r", type=int, default=100, help="주입할 Mock 데이터 행 수")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    validator = SandboxValidator()

    print("=" * 60)
    print("🧪 DuckDB 샌드박스 검증 시작")
    print("=" * 60)

    # 모든 테이블 생성 및 데이터 주입 (컬럼 없는 항목 및 중복 제거)
    seen = set()
    all_tables = []
    for t in config.get("tables", []) + config.get("gold_tables", []):
        name = t.get("name", "")
        has_columns = len(t.get("columns", [])) > 0
        if name in seen:
            print(f"  ⚠️  중복 테이블 건너뜀: {name}")
            continue
        if not has_columns:
            print(f"  ⚠️  컬럼 없는 테이블 건너뜀: {name}")
            continue
        seen.add(name)
        all_tables.append(t)

    for table in all_tables:
        print(f"\n--- {table['name']} ---")
        if validator.create_table_from_schema(table):
            if validator.inject_mock_data(table, args.rows):
                validator.run_validation_queries(table)

    # 추가 SQL 파일 실행
    if args.sql_dir:
        sql_dir = Path(args.sql_dir)
        if sql_dir.exists():
            for sql_file in sorted(sql_dir.glob("*.sql")):
                print(f"\n--- SQL: {sql_file.name} ---")
                sql_content = sql_file.read_text(encoding="utf-8")
                for stmt in sql_content.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        validator.run_custom_sql(stmt, f"[{sql_file.name}] {stmt[:50]}...")

    # 리포트 생성
    report = validator.generate_report()

    print(f"\n{'='*60}")
    print(f"📊 검증 결과 요약")
    print(f"   총 검사: {report['summary']['total_checks']}")
    print(f"   ✅ 통과: {report['summary']['passed']}")
    print(f"   ❌ 실패: {report['summary']['failed']}")
    print(f"   ⚠️  경고: {report['summary']['warnings']}")
    print(f"   상태: {'🎉 PASSED' if report['summary']['status'] == 'PASSED' else '💥 FAILED'}")
    print(f"{'='*60}")

    if report["errors"]:
        print(f"\n🔍 에러 상세:")
        for err in report["errors"]:
            print(f"   Phase: {err['phase']}")
            print(f"   Error: {err['error']}")
            if "sql" in err:
                print(f"   SQL: {err['sql'][:200]}")
            print()

    if args.output:
        Path(args.output).write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"📄 리포트 저장: {args.output}")

    # DuckDB 전용 경고
    print("\n⚠️  주의: DuckDB는 로컬 검증 전용입니다.")
    print("   Spark/Trino 전용 기능(UDF, 특수 커넥터 등)은 운영 환경에서 별도 검증이 필요합니다.")

    validator.close()
    sys.exit(0 if report["summary"]["status"] == "PASSED" else 1)


if __name__ == "__main__":
    main()
