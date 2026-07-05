import sqlite3
import time
import subprocess
import signal
from datetime import datetime, timedelta

DB_FILE = "queue.db"
BACKOFF_BASE = 2

# This is our global kill-switch flag
keep_running = True

def handle_shutdown(signum, frame):
    """Catches Ctrl+C and flips the switch instead of crashing."""
    global keep_running
    print("\n[Signal] Shutdown requested! Worker will exit after the current job finishes...")
    keep_running = False

def start_worker(worker_id=1):
    global keep_running
    
    # Tell Python to route Ctrl+C (SIGINT) to our custom function
    signal.signal(signal.SIGINT, handle_shutdown)
    
    print(f"Worker {worker_id} started! Monitoring queue...")
    
    # Notice we changed 'while True:' to 'while keep_running:'
    while keep_running:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()

            cursor.execute("BEGIN IMMEDIATE")

            now_str = datetime.utcnow().isoformat() + "Z"

            cursor.execute('''
                SELECT * FROM jobs 
                WHERE state = 'pending' OR (state = 'failed' AND run_after <= ?)
                ORDER BY created_at ASC 
                LIMIT 1
            ''', (now_str,))
            
            job = cursor.fetchone()

            if not job:
                conn.commit()
                conn.close()
                time.sleep(2)
                continue # Loop restarts, will break if keep_running is False

            job_id = job['id']
            command = job['command']
            current_attempts = job['attempts']
            max_retries = job['max_retries']

            cursor.execute('''
                UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ?
            ''', (now_str, job_id))
            conn.commit() 
            conn.close()

            print(f"\n[Worker {worker_id}] Processing '{job_id}' (Attempt {current_attempts + 1}/{max_retries + 1})")
            
            # The worker is executing! If Ctrl+C is pressed NOW, the script won't die. 
            # It waits for this subprocess to finish, and breaks the loop on the next cycle!
            result = subprocess.run(command, shell=True)
            
            now = datetime.utcnow()
            now_str = now.isoformat() + "Z"
            
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()

            if result.returncode == 0:
                cursor.execute('''
                    UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?
                ''', (now_str, job_id))
                print(f"[Worker {worker_id}] Job '{job_id}' completed successfully!")
            else:
                new_attempts = current_attempts + 1
                if new_attempts > max_retries:
                    cursor.execute('''
                        UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ? WHERE id = ?
                    ''', (new_attempts, now_str, job_id))
                    print(f"[Worker {worker_id}] Job '{job_id}' moved to DLQ.")
                else:
                    delay_seconds = BACKOFF_BASE ** new_attempts
                    run_after_time = now + timedelta(seconds=delay_seconds)
                    run_after_str = run_after_time.isoformat() + "Z"
                    cursor.execute('''
                        UPDATE jobs SET state = 'failed', attempts = ?, run_after = ?, updated_at = ? WHERE id = ?
                    ''', (new_attempts, run_after_str, now_str, job_id))
                    print(f"[Worker {worker_id}] Job '{job_id}' failed. Retry in {delay_seconds}s.")

            conn.commit()
            conn.close()

        except sqlite3.OperationalError:
            time.sleep(0.5)
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(2)
            
    print(f"Worker {worker_id} has safely shut down.")