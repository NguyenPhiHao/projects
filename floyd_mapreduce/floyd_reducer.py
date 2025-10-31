#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import math

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

def fmt(d):
    if d == math.inf:
        return "INF"
    if float(d).is_integer():
        return str(int(d))
    return str(round(d, 2))

def flush_i(i_key, best_by_j):
    if i_key is None:
        return
    for j in sorted(best_by_j.keys(), key=lambda x: int(x)):
        d, p = best_by_j[j]
        print(f"{i_key}\t{j}\t{fmt(d)}|{p}")

current_i = None
best_by_j = {}  # j -> (best_d, best_path)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.replace("\t", " ").split()
    if len(parts) < 3:
        continue

    i, j = parts[0], parts[1]
    d, p = parse_value(parts[2])

    if current_i is None:
        current_i = i
    elif i != current_i:
        flush_i(current_i, best_by_j)
        current_i = i
        best_by_j = {}

    prev = best_by_j.get(j, (math.inf, ""))
    if d < prev[0]:
        best_by_j[j] = (d, p)

flush_i(current_i, best_by_j)
