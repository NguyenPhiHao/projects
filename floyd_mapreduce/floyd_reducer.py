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
    if d.is_integer():
        return str(int(d))
    return str(round(d, 2))

current = None
best_d = math.inf
best_path = ""

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.replace("\t", " ").split()
    if len(parts) < 3:
        continue
    key = (parts[0], parts[1])
    d, p = parse_value(parts[2])
    if current is None:
        current = key
        best_d, best_path = d, p
        continue
    if key != current:
        print(f"{current[0]}\t{current[1]}\t{fmt(best_d)}|{best_path}")
        current = key
        best_d, best_path = d, p
    else:
        if d < best_d:
            best_d, best_path = d, p

if current is not None:
    print(f"{current[0]}\t{current[1]}\t{fmt(best_d)}|{best_path}")
