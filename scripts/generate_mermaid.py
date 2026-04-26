#!/usr/bin/env python3
"""
Mermaid 데이터 계보(Lineage) 다이어그램 생성 스크립트 (Phase 4)
설계 구성 JSON을 입력받아 Bronze → Silver → Gold 데이터 흐름을 시각화합니다.
"""

import argparse
import json
import sys
from pathlib import Path


def build_mermaid(config: dict) -> str:
    """설계 구성에서 Mermaid flowchart를 생성"""
    lines = ["```mermaid", "flowchart LR"]
    lines.append("")

    # 스타일 정의
    lines.append("    classDef bronze fill:#cd7f32,stroke:#8b4513,color:#fff")
    lines.append("    classDef silver fill:#c0c0c0,stroke:#808080,color:#000")
    lines.append("    classDef gold fill:#ffd700,stroke:#daa520,color:#000")
    lines.append("")

    sources = config.get("sources", [])
    tables = config.get("tables", [])
    gold_tables = config.get("gold_tables", [])

    bronze_nodes = []
    silver_nodes = []
    gold_nodes = []

    # Bronze 노드 (소스)
    for src in sources:
        node_id = f"bronze_{_sanitize(src['name'])}"
        label = f"{src['name']}<br/>📥 {src.get('origin', 'Source')}<br/>({src.get('load_method', 'Batch')})"
        lines.append(f'    {node_id}["{label}"]')
        bronze_nodes.append({"id": node_id, "name": src["name"]})
    lines.append("")

    # Silver/Gold 노드 (tables 배열에서)
    for tbl in tables:
        node_id = f"{tbl['layer']}_{_sanitize(tbl['name'])}"
        mode = tbl.get("mode", "?")
        partition_info = ", ".join(
            f"{p['transform']}({p['column']})" for p in tbl.get("partition_spec", [])
        )
        label = f"{tbl['name']}<br/>⚙️ {mode}<br/>🗂 {partition_info or 'no partition'}"
        lines.append(f'    {node_id}["{label}"]')
        if tbl["layer"] == "silver":
            silver_nodes.append({"id": node_id, "name": tbl["name"]})
        elif tbl["layer"] == "gold":
            gold_nodes.append({"id": node_id, "name": tbl["name"]})
    lines.append("")

    # Gold 테이블 (gold_tables가 별도로 정의된 경우)
    for gt in gold_tables:
        node_id = f"gold_{_sanitize(gt['name'])}"
        col_count = len(gt.get("columns", []))
        label = f"{gt['name']}<br/>📊 Gold Table<br/>({col_count} columns)"
        lines.append(f'    {node_id}["{label}"]')
        gold_nodes.append({"id": node_id, "name": gt["name"]})
    lines.append("")

    # 엣지 생성: Bronze → Silver
    lineage = config.get("lineage", [])
    if lineage:
        for edge in lineage:
            src_id = _find_node_id(edge["from"], bronze_nodes + silver_nodes + gold_nodes)
            tgt_id = _find_node_id(edge["to"], bronze_nodes + silver_nodes + gold_nodes)
            transform = edge.get("transform", "")
            if src_id and tgt_id:
                if transform:
                    lines.append(f"    {src_id} -->|{transform}| {tgt_id}")
                else:
                    lines.append(f"    {src_id} --> {tgt_id}")
    else:
        # 자동 연결: Bronze → Silver → Gold
        for bn in bronze_nodes:
            for sn in silver_nodes:
                lines.append(f"    {bn['id']} --> {sn['id']}")
        for sn in silver_nodes:
            for gn in gold_nodes:
                lines.append(f"    {sn['id']} --> {gn['id']}")

    lines.append("")

    # 클래스 적용
    for n in bronze_nodes:
        lines.append(f"    class {n['id']} bronze")
    for n in silver_nodes:
        lines.append(f"    class {n['id']} silver")
    for n in gold_nodes:
        lines.append(f"    class {n['id']} gold")

    lines.append("```")
    return "\n".join(lines)


def build_column_mapping(config: dict) -> str:
    """소스→타겟 컬럼 매핑 테이블 생성"""
    mappings = config.get("column_mappings", [])
    if not mappings:
        return ""

    lines = [
        "## 컬럼 매핑 (Source → Target)",
        "",
        "| Source Table | Source Column | Transform | Target Table | Target Column | Type |",
        "|---|---|---|---|---|---|",
    ]

    for m in mappings:
        lines.append(
            f"| {m.get('source_table', '-')} "
            f"| {m.get('source_column', '-')} "
            f"| `{m.get('transform', 'direct')}` "
            f"| {m.get('target_table', '-')} "
            f"| {m.get('target_column', '-')} "
            f"| {m.get('type', '-')} |"
        )

    return "\n".join(lines)


def _sanitize(name: str) -> str:
    return name.replace("-", "_").replace(".", "_").replace(" ", "_")


def _find_node_id(name: str, nodes: list) -> str | None:
    for n in nodes:
        if n["name"] == name:
            return n["id"]
    return None


def main():
    parser = argparse.ArgumentParser(description="Mermaid 데이터 계보 다이어그램 생성")
    parser.add_argument("--config", "-c", required=True, help="설계 구성 JSON 파일 경로")
    parser.add_argument("--output", "-o", default=None, help="출력 마크다운 파일 (미지정 시 stdout)")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))

    sections = []
    sections.append("# 데이터 계보 (Data Lineage)")
    sections.append("")
    sections.append(build_mermaid(config))
    sections.append("")

    col_mapping = build_column_mapping(config)
    if col_mapping:
        sections.append(col_mapping)

    output_text = "\n".join(sections)

    if args.output:
        Path(args.output).write_text(output_text, encoding="utf-8")
        print(f"✅ 다이어그램 저장 완료: {args.output}")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
