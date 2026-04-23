%%writefile tugas_uts_final.py
from mpi4py import MPI
import threading
import time
import random

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

NUM_THREADS = 2

# -----------------------------
# DATA (Kasus nyata)
# -----------------------------
random.seed(42)
DATA = [random.randint(1, 100) for _ in range(20)]  # data angka
TASKS = list(range(len(DATA)))  # index data sebagai tugas

# -----------------------------
# THREAD WORKER
# -----------------------------
def thread_worker(thread_id, task_list, results, lock):
    local_sum = 0

    # Guided internal (thread level)
    while task_list:
        with lock:
            if not task_list:
                break
            chunk_size = max(1, len(task_list) // (2 * NUM_THREADS))
            chunk = task_list[:chunk_size]
            del task_list[:chunk_size]

        for idx in chunk:
            value = DATA[idx]
            result = value * 2  # contoh operasi (kasus nyata)
            time.sleep(0.1)  # simulasi kerja
            local_sum += result

            print(f"    [Rank {rank} | Thread {thread_id}] "
                  f"Data[{idx}]={value} → {result}")

    results[thread_id] = local_sum


# -----------------------------
# MASTER
# -----------------------------
if rank == 0:
    print(f"[MASTER] Total data: {len(DATA)}")
    print(f"[MASTER] Data: {DATA}\n")

    task_index = 0
    completed = 0
    total_result = 0

    # Kirim tugas awal
    for worker in range(1, size):
        if task_index < len(TASKS):
            remaining = len(TASKS) - task_index
            chunk_size = max(1, remaining // (2 * (size - 1)))

            chunk = TASKS[task_index:task_index + chunk_size]
            comm.send(chunk, dest=worker, tag=1)

            print(f"[MASTER] Kirim {chunk} ke Worker-{worker}")

            task_index += chunk_size

    # Loop guided scheduling
    while completed < len(TASKS):
        status = MPI.Status()
        result = comm.recv(source=MPI.ANY_SOURCE, tag=2, status=status)

        worker_id = status.Get_source()
        completed += result[1]  # jumlah task selesai
        total_result += result[0]

        print(f"[MASTER] Terima hasil dari Worker-{worker_id} | "
              f"Progress: {completed}/{len(TASKS)}")

        if task_index < len(TASKS):
            remaining = len(TASKS) - task_index
            chunk_size = max(1, remaining // (2 * (size - 1)))

            chunk = TASKS[task_index:task_index + chunk_size]
            comm.send(chunk, dest=worker_id, tag=1)

            print(f"[MASTER] Kirim {chunk} ke Worker-{worker_id}")

            task_index += chunk_size
        else:
            comm.send(None, dest=worker_id, tag=0)

    print("\n[MASTER] Semua tugas selesai")
    print(f"[MASTER] Total hasil akhir: {total_result}")


# -----------------------------
# WORKER
# -----------------------------
else:
    while True:
        status = MPI.Status()
        task_chunk = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

        if status.Get_tag() == 0:
            print(f"[Worker-{rank}] Berhenti")
            break

        print(f"\n[Worker-{rank}] Dapat chunk: {task_chunk}")

        # Copy task biar thread bisa modif
        local_tasks = task_chunk.copy()

        results = [0] * NUM_THREADS
        lock = threading.Lock()

        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(
                target=thread_worker,
                args=(i, local_tasks, results, lock)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        local_sum = sum(results)
        task_done = len(task_chunk)

        print(f"[Worker-{rank}] Selesai chunk | hasil: {local_sum}")

        comm.send((local_sum, task_done), dest=0, tag=2) 