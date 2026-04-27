#!/usr/bin/env python3
"""
소스 데이터 프로파일링 스크립트 (Phase 2)
CSV/Parquet/JSON 파일을 분석하여 카디널리티, Null 비율, 데이터 분포 등을 리포트합니다.
"""

import argparse
import json
import sys
from pathlib import Path

# ── venv 강제 실행 가드 ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from venv_guard import ensure_venv
ensure_venv()
# ──────────────────────────────────────────────────────────

try:
    import pandas as pd
except ImportError:
    print("pandas가 설치되지 않았습니다. 설치 중...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "pyarrow", "-q"])
    import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    p = Path(file_path)
    suffix = p.suffix.lower()
    loaders = {
        ".csv": lambda: pd.read_csv(p),
        ".tsv": lambda: pd.read_csv(p, sep="\t"),
        ".parquet": lambda: pd.read_parquet(p),
        ".json": lambda: pd.read_json(p),
        ".jsonl": lambda: pd.read_json(p, lines=True),
    }
    loader = loaders.get(suffix)
    if not loader:
        raise ValueError(f"지원하지 않는 파일 형식: {suffix} (지원: csv, tsv, parquet, json, jsonl)")
    return loader()


def infer_semantic_type(series: pd.Series, col_name: str) -> str:
    """컬럼 이름과 데이터 패턴으로 의미론적 타입을 추론"""
    name_lower = col_name.lower()

    ts_keywords = ["ts", "timestamp", "datetime", "created", "updated", "time", "date", "_at"]
    if any(kw in name_lower for kw in ts_keywords):
        return "TIMESTAMP_CANDIDATE"

    id_keywords = ["_id", "_key", "_code", "_no", "_num", "serial"]
    if any(kw in name_lower for kw in id_keywords):
        return "IDENTIFIER"

    if pd.api.types.is_numeric_dtype(series):
        return "MEASURE"

    return "ATTRIBUTE"


def classify_cardinality(nunique: int, total: int) -> str:
    if total == 0:
        return "empty"
    ratio = nunique / total
    if ratio > 0.95:
        return "unique"
    if ratio > 0.5:
        return "high"
    if ratio > 0.05:
        return "medium"
    return "low"


def suggest_partition_strategy(col_profile: dict) -> dict | None:
    """카디널리티와 타입에 따라 파티션 전략을 제안"""
    sem_type = col_profile["semantic_type"]
    card = col_profile["cardinality_class"]

    if sem_type == "TIMESTAMP_CANDIDATE":
        return {"transform": "days", "reason": "시계열 데이터 — days() 파티셔닝 권장"}

    if sem_type == "IDENTIFIER" and card == "low":
        return {"transform": "identity", "reason": f"낮은 카디널리티({col_profile['nunique']}) — identity 파티셔닝 가능"}

    if sem_type == "IDENTIFIER" and card == "medium":
        bucket_count = min(64, max(8, col_profile["nunique"] // 100))
        return {"transform": f"bucket({bucket_count})", "reason": f"중간 카디널리티 — bucket({bucket_count}) 권장"}

    if sem_type == "IDENTIFIER" and card in ("high", "unique"):
        return {"transform": "SORT_ONLY", "reason": "카디널리티가 너무 높아 파티션 부적합 → Sort/Z-Order 권장"}

    return None


def profile_column(series: pd.Series, col_name: str) -> dict:
    total = len(series)
    null_count = int(series.isna().sum())
    non_null = series.dropna()
    nunique = int(non_null.nunique())

    profile = {
        "name": col_name,
        "dtype": str(series.dtype),
        "semantic_type": infer_semantic_type(series, col_name),
        "total_count": total,
        "null_count": null_count,
        "null_ratio": round(null_count / total, 4) if total > 0 else 0,
        "nunique": nunique,
        "cardinality_class": classify_cardinality(nunique, total),
    }

    if pd.api.types.is_numeric_dtype(series) and len(non_null) > 0:
        desc = non_null.describe()
        profile["stats"] = {
            "min": float(desc["min"]),
            "max": float(desc["max"]),
            "mean": round(float(desc["mean"]), 4),
            "std": round(float(desc["std"]), 4),
            "median": round(float(non_null.median()), 4),
        }

    if profile["cardinality_class"] in ("low", "medium") and nunique <= 30:
        profile["top_values"] = (
            non_null.value_counts().head(10).to_dict()
        )
        # JSON 직렬화를 위해 키를 문자열로 변환
        profile["top_values"] = {str(k): v for k, v in profile["top_values"].items()}

    partition_suggestion = suggest_partition_strategy(profile)
    if partition_suggestion:
        profile["partition_suggestion"] = partition_suggestion

    return profile


def profile_dataframe(df: pd.DataFrame, source_name: str) -> dict:
    columns = [profile_column(df[col], col) for col in df.columns]

    partition_candidates = [
        c["name"] for c in columns
        if c.get("partition_suggestion") and c["partition_suggestion"]["transform"] != "SORT_ONLY"
    ]
    sort_candidates = [
        c["name"] for c in columns
        if c.get("partition_suggestion") and c["partition_suggestion"]["transform"] == "SORT_ONLY"
    ]

    # 중복 행 탐지
    duplicate_count = int(df.duplicated().sum())

    return {
        "source_name": source_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_rows": duplicate_count,
        "duplicate_ratio": round(duplicate_count / len(df), 4) if len(df) > 0 else 0,
        "columns": columns,
        "summary": {
            "partition_candidates": partition_candidates,
            "sort_candidates": sort_candidates,
            "quality_warnings": _detect_quality_issues(columns, duplicate_count),
        },
    }


def _detect_quality_issues(columns: list, duplicate_count: int) -> list:
    warnings = []
    for col in columns:
        if col["null_ratio"] > 0.5:
            warnings.append(f"'{col['name']}' — Null 비율 {col['null_ratio']*100:.1f}%로 매우 높음")
        if col["null_ratio"] > 0 and col["semantic_type"] == "IDENTIFIER":
            warnings.append(f"'{col['name']}' — 식별자 컬럼에 Null 존재 ({col['null_count']}건)")
    if duplicate_count > 0:
        warnings.append(f"중복 행 {duplicate_count}건 발견")
    return warnings


def main():
    parser = argparse.ArgumentParser(description="소스 데이터 프로파일링")
    parser.add_argument("--input", "-i", required=True, help="입력 파일 경로 (csv, parquet, json, jsonl)")
    parser.add_argument("--output", "-o", default=None, help="결과 JSON 파일 경로 (미지정 시 stdout)")
    parser.add_argument("--name", "-n", default=None, help="소스 이름 (미지정 시 파일명 사용)")
    parser.add_argument("--sample", "-s", type=int, default=None, help="샘플 행 수 (대용량 파일용)")
    args = parser.parse_args()

    print(f"📂 파일 로드 중: {args.input}")
    df = load_data(args.input)

    if args.sample and len(df) > args.sample:
        print(f"📊 샘플링: {len(df):,}행 → {args.sample:,}행")
        df = df.sample(n=args.sample, random_state=42)

    source_name = args.name or Path(args.input).stem
    print(f"🔍 프로파일링 수행 중: {source_name} ({len(df):,}행, {len(df.columns)}컬럼)")

    result = profile_dataframe(df, source_name)

    output_json = json.dumps(result, ensure_ascii=False, indent=2, default=str)

    if args.output:
        Path(args.output).write_text(output_json, encoding="utf-8")
        print(f"✅ 프로파일 저장 완료: {args.output}")
    else:
        print("\n" + output_json)

    # 요약 출력
    summary = result["summary"]
    print(f"\n{'='*60}")
    print(f"📋 요약")
    print(f"   파티션 후보: {', '.join(summary['partition_candidates']) or '없음'}")
    print(f"   정렬 후보:   {', '.join(summary['sort_candidates']) or '없음'}")
    if summary["quality_warnings"]:
        print(f"   ⚠️  품질 경고:")
        for w in summary["quality_warnings"]:
            print(f"      - {w}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
