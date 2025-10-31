#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import subprocess
import sys

# ==================== CẤU HÌNH ====================
HADOOP_STREAMING = r"C:\hadoop-3.3.0\share\hadoop\tools\lib\hadoop-streaming-3.3.0.jar"
PYTHON = r"C:\Users\Admin\AppData\Local\Programs\Python\Python313\python.exe"
BASE_HDFS = "/floyd_warshall"
LOCAL_INPUT = r"C:\hadoop-3.3.0\projects\floyd_mapreduce\input_fw_undirected.txt"
LOCAL_WORKDIR = r"C:\hadoop-3.3.0\projects\floyd_mapreduce"
FW_LOCAL_RESULT = os.path.join(LOCAL_WORKDIR, "fw_result.txt")
# ==================================================

def run_cmd(cmd, check=False):
    print("👉", cmd)
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode != 0:
        print("❌ Lỗi:", res.stderr.strip())
        if check:
            sys.exit(1)
        return False
    return True

# ==================== TẠO INPUT ĐẦY ĐỦ ====================
def get_nodes_from_input():
    """Lấy danh sách các đỉnh từ file input_fw_undirected.txt"""
    nodes = set()
    with open(LOCAL_INPUT, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                try:
                    u = int(parts[0]); v = int(parts[1])
                    nodes.add(u); nodes.add(v)
                except:
                    continue
    return sorted(nodes)

def make_full_input_temp(nodes):
    """Sinh ma trận đầy đủ (vô hướng)"""
    temp_file = os.path.join(LOCAL_WORKDIR, "input_full_auto.txt")
    edges = {}
    with open(LOCAL_INPUT, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            try:
                u, v, w = int(parts[0]), int(parts[1]), float(parts[2])
                edges[(u, v)] = w
                if (v, u) not in edges and u != v:
                    edges[(v, u)] = w
            except:
                continue

    with open(temp_file, 'w', encoding='utf-8') as f:
        for i in nodes:
            for j in nodes:
                if i == j:
                    f.write(f"{i} {j} 0.0\n")
                elif (i, j) in edges:
                    f.write(f"{i} {j} {edges[(i, j)]}\n")
                else:
                    f.write(f"{i} {j} INF\n")
    print(f"✅ Đã sinh file đầy đủ (vô hướng): {temp_file}")
    return temp_file

def hdfs_put(full_input_path):
    run_cmd(f'hdfs dfs -rm -r -skipTrash "{BASE_HDFS}"')
    run_cmd(f'hdfs dfs -mkdir -p "{BASE_HDFS}/input"')
    run_cmd(f'hdfs dfs -put -f "{full_input_path}" "{BASE_HDFS}/input/input_full_auto.txt"', check=True)

# ==================== CHẠY MAPREDUCE ====================
def run_iteration(k, input_hdfs, output_hdfs):
    mapper = f'"{PYTHON} floyd_mapper.py {k}"'
    reducer = f'"{PYTHON} floyd_reducer.py {k}"'
    files_list = (
        "file:///C:/hadoop-3.3.0/projects/floyd_mapreduce/floyd_mapper.py,"
        "file:///C:/hadoop-3.3.0/projects/floyd_mapreduce/floyd_reducer.py"
    )
    cmd = (
        f'hadoop jar "{HADOOP_STREAMING}" '
        f'-D mapreduce.job.maps=1 '       # thêm dòng này
        f'-D mapreduce.job.reduces=1 '
        f'-files "{files_list}" '
        f'-mapper {mapper} -reducer {reducer} '
        f'-input "{input_hdfs}" -output "{output_hdfs}"'
    )
    return run_cmd(cmd)

def fetch_result_to_local(hdfs_output_dir, local_file):
    hdfs_part = f"{hdfs_output_dir}/part-00000"
    ok = run_cmd(f'hdfs dfs -test -e "{hdfs_part}"')
    if not ok:
        print(f"⚠️ Không tìm thấy {hdfs_part}")
        return False
    cmd = f'hdfs dfs -cat "{hdfs_part}" > "{local_file}"'
    return run_cmd(cmd)

# ==================== CHƯƠNG TRÌNH CHÍNH ====================
def main():
    os.chdir(LOCAL_WORKDIR)
    nodes = get_nodes_from_input()
    if not nodes:
        print("⚠️ Không có dữ liệu input hợp lệ.")
        return

    # 1️⃣ Chuẩn bị input
    full_input_path = make_full_input_temp(nodes)
    hdfs_put(full_input_path)

    print(f"\n=== Bắt đầu Floyd–Warshall (n = {len(nodes)}) ===")

    # 2️⃣ Chạy tuần tự theo k
    input_hdfs = f"{BASE_HDFS}/input/input_full_auto.txt"
    last_output = None

    for idx, k_label in enumerate(nodes, start=1):
        output_hdfs = f"{BASE_HDFS}/iter_{idx}"
        run_cmd(f'hdfs dfs -rm -r -skipTrash "{output_hdfs}"')
        print(f"\n=== Vòng k = {k_label} ===")
        ok = run_iteration(k_label, input_hdfs, output_hdfs)
        if not ok:
            print(f"❌ Job vòng k={k_label} thất bại.")
            return
        last_output = output_hdfs
        input_hdfs = output_hdfs

    # 3️⃣ Vòng cuối (đồng bộ toàn đồ thị)
    final_output = f"{BASE_HDFS}/iter_final"
    run_cmd(f'hdfs dfs -rm -r -skipTrash "{final_output}"')
    print("\n=== Vòng cuối (lan truyền toàn bộ đường đi) ===")
    if run_iteration(len(nodes) + 1, last_output, final_output):
        last_output = final_output

    # 4️⃣ Kết quả cuối
    if last_output:
        print("\n✅ Hoàn tất. Tải kết quả về local:", FW_LOCAL_RESULT)
        fetch_result_to_local(last_output, FW_LOCAL_RESULT)
    else:
        print("❌ Không có output cuối cùng.")

if __name__ == "__main__":
    main()
