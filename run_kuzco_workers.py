import argparse
import subprocess
import time
import threading
import signal
import psutil
from datetime import datetime, timedelta

stop_flag = threading.Event()

def terminate_process(process, worker_id):
    if process is None:
        return

    print(f"Worker {worker_id}: Terminating process...")
    try:
        # Get the process and its children
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)

        # Terminate children
        for child in children:
            child.terminate()
        parent.terminate()

        # Wait for processes to terminate
        gone, alive = psutil.wait_procs(children + [parent], timeout=3)

        # Force kill any remaining processes
        for p in alive:
            print(f"Worker {worker_id}: Force killing process {p.pid}")
            p.kill()

    except psutil.NoSuchProcess:
        print(f"Worker {worker_id}: Process already terminated")
    except Exception as e:
        print(f"Worker {worker_id}: Error while terminating process - {str(e)}")

    print(f"Worker {worker_id}: Process termination completed")

def run_worker(command, worker_id, silent, no_inference_timeout):
    process = None
    last_inference_time = datetime.now()

    while not stop_flag.is_set():
        if process is None or process.poll() is not None:
            if process is not None:
                print(f"Worker {worker_id}: Restarting")
                terminate_process(process, worker_id)

            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True)
            last_inference_time = datetime.now()

        try:
            # Read output with a timeout
            output = process.stdout.readline()
            if output:
                if not silent:
                    print(f"Worker {worker_id}: {output.strip()}")
                
                # Update last inference time if needed
                if 'Inference finished' in output:
                    last_inference_time = datetime.now()
            else:
                # Check if the process is unresponsive
                if process.poll() is None:
                    if datetime.now() - last_inference_time > timedelta(minutes=no_inference_timeout):
                        print(f"Worker {worker_id}: No inference finished for {no_inference_timeout} minutes. Restarting...")
                        terminate_process(process, worker_id)
                        process = None
                        last_inference_time = datetime.now()

        except Exception as e:
            print(f"Worker {worker_id}: Error - {str(e)}. Restarting...")
            terminate_process(process, worker_id)
            process = None
            time.sleep(5)  # Wait before restarting

    if process:
        print(f"Worker {worker_id}: Stopping")
        terminate_process(process, worker_id)

def signal_handler(signum, frame):
    print("\nCtrl+C pressed. Stopping all workers...")
    stop_flag.set()

def restart_all_workers(threads, command, silent, no_inference_timeout):
    print("Restarting all workers...")
    stop_flag.set()  # Signal all workers to stop
    for thread in threads:
        thread.join()  # Wait for all workers to finish
    stop_flag.clear()
    
    new_threads = []
    for i in range(len(threads)):
        thread = threading.Thread(target=run_worker, args=(command, i, silent, no_inference_timeout))
        thread.start()
        new_threads.append(thread)
        time.sleep(1)  # 1-second pause between starting each worker
    return new_threads

def main():
    parser = argparse.ArgumentParser(description="Run Kuzco workers in parallel")
    parser.add_argument("command", help="Kuzco worker command to run")
    parser.add_argument("instances", type=int, help="Number of instances to run in parallel")
    parser.add_argument("--silent", action="store_true", help="Enable silent mode (only output logs about starting/restarting workers)")
    parser.add_argument("--no-inference-timeout", type=int, default=60, help="Timeout in minutes for no inference before restarting")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    if not args.silent:
        print(f"Starting {args.instances} workers")

    threads = []
    for i in range(args.instances):
        thread = threading.Thread(target=run_worker, args=(args.command, i, args.silent, args.no_inference_timeout))
        thread.start()
        threads.append(thread)
        time.sleep(1)  # 1-second pause between starting each worker

    try:
        while True:
            time.sleep(300)  # 5-minute interval
            threads = restart_all_workers(threads, args.command, args.silent, args.no_inference_timeout)
    except KeyboardInterrupt:
        print("Stopping all workers...")
        stop_flag.set()
        for thread in threads:
            thread.join()
        if not args.silent:
            print("All workers have finished")

if __name__ == "__main__":
    main()
