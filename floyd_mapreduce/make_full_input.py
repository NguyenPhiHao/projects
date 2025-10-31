#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_full_input.py
------------------
Tạo file input đầy đủ cho thuật toán Floyd–Warshall MapReduce
Từ file cạnh (chỉ chứa u v w) → sinh ma trận n×n (u v dist|path)

Cách dùng:
    python make_full_input.py edges.txt input_full.txt
"""

import sys

INF = 1e9  # giá trị đại diện cho vô cực (INF)

def read_edges(file_path):
    """Đọc file danh sách cạnh (u v w)."""
    edges = {}
    nodes = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split()
                if len(parts) < 3:
                    continue
                try:
                    u = int(parts[0])
                    v = int(parts[1])
                    w = float(parts[2])
                except ValueError:
                    continue
                edges[(u, v)] = w
                nodes.add(u)
                nodes.add(v)
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {file_path}")
        sys.exit(1)
    return edges, sorted(nodes)

def write_full_matrix(edges, nodes, output_path):
    """Ghi file ma trận đầy đủ u v dist|path"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for u in nodes:
            for v in nodes:
                if u == v:
                    f.write(f"{u} {v} 0.0|{u}\n")
                elif (u, v) in edges:
                    w = edges[(u, v)]
                    f.write(f"{u} {v} {w}|{u}->{v}\n")
                else:
                    f.write(f"{u} {v} {INF}| \n")
    print(f"✅ Đã tạo file full matrix tại: {output_path}")
    print(f"🔢 Tổng số đỉnh: {len(nodes)}, tổng số cặp: {len(nodes) ** 2}")

def main():
    if len(sys.argv) < 3:
        print("⚙️  Cách dùng:")
        print("    python make_full_input.py edges.txt input_full.txt")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"📖 Đang đọc danh sách cạnh từ: {input_file}")
    edges, nodes = read_edges(input_file)
    if not nodes:
        print("⚠️  Không có cạnh hợp lệ trong file.")
        sys.exit(1)

    print(f"✏️  Đang sinh ma trận đầy đủ với {len(nodes)} đỉnh...")
    write_full_matrix(edges, nodes, output_file)

if __name__ == "__main__":
    main()
