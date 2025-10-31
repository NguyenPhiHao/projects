#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import math

INF = float("inf")

def parse_value(val):
    if "|" in val:
        d_str, path = val.split("|", 1)
    else:
        d_str, path = val, ""
    d_str = d_str.strip().upper()
    if d_str in ("INF", "INFINITY", "∞"):
        return (math.inf, path.strip())
    try:
        return (float(d_str), path.strip())
    except:
        return (math.inf, path.strip())

def normalize_path(path):
    parts = [p.strip() for p in path.split("->") if p.strip()]
    if not parts:
        return ""
    new = [parts[0]]
    for p in parts[1:]:
        if p != new[-1]:
            new.append(p)
    return "->".join(new)

def fmt(d):
    if d == math.inf:
        return "INF"
    if d.is_integer():
        return str(int(d))
    return str(round(d, 2))

def main():
    if len(sys.argv) < 2:
        print("Usage: floyd_mapper.py <k>", file=sys.stderr)
        sys.exit(1)
    k = int(sys.argv[1])

    graph = {}
    nodes = set()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.replace("\t", " ").split()
        if len(parts) < 3:
            continue
        try:
            u, v = int(parts[0]), int(parts[1])
        except:
            continue
        dist, path = parse_value(parts[2])
        if not path:
            path = f"{u}->{v}" if u != v else f"{u}"
        graph[(u, v)] = (dist, normalize_path(path))
        nodes.add(u)
        nodes.add(v)

    all_nodes = sorted(nodes)

    # --- Nếu k=0 hoặc lớn hơn n: chỉ phát lại dữ liệu ---
    if k == 0 or k > max(all_nodes):
        for (i, j), (d, p) in sorted(graph.items()):
            print(f"{i}\t{j}\t{fmt(d)}|{p}")
        return

    # --- Phát lại dữ liệu gốc ---
    for (i, j), (d, p) in sorted(graph.items()):
        print(f"{i}\t{j}\t{fmt(d)}|{p}")

    # --- Sinh candidate i -> j qua k ---
    for i in all_nodes:
        for j in all_nodes:
            if i == j:
                continue
            dik, pik = graph.get((i, k), (math.inf, f"{i}->{k}"))
            dkj, pkj = graph.get((k, j), (math.inf, f"{k}->{j}"))
            if dik == math.inf or dkj == math.inf:
                continue
            new_d = dik + dkj
            if "->" in pkj:
                tail = pkj.split("->", 1)[1]
                new_path = f"{pik}->{tail}"
            else:
                new_path = f"{pik}->{k}->{j}"
            new_path = normalize_path(new_path)
            print(f"{i}\t{j}\t{fmt(new_d)}|{new_path}")

if __name__ == "__main__":
    main()
