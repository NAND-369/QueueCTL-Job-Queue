import sqlite3
import time
import subprocess
from datetime import datetime, timedelta

DB_FILE = "queue.db"
BACKOFF_BASE = 2  # The 'base' value for our delay calculation

def start_worker():
    print("Worker started! Monitoring queue for active or retryable jobs...")
    
    while True:
        try:
            conn = sqlite3.connect(DB_FILE, timeout=10)
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()

            # Protect against race conditions
            cursor.execute("BEGIN IMMEDIATE")

            # Get current time in ISO format
            now_str = datetime.utcnow().isoformat() + "Z"

            # UPGRADED SELECT: Look for 'pending' jobs OR 'failed' jobs whose wait time has expired
            cursor.execute('''
                SELECT * FROM jobs 
                WHERE state = 'pending' OR (state = 'failed' AND run_after <= ?)
                ORDER BY created_at ASC 
                LIMIT 1
            ''', (now_str,))
            
            job = cursor.fetchone()

            if not job:
                conn.commit()
                time.sleep(2)
                continue

            job_id = job['id']
            command = job['command']
            current_attempts = job['attempts']
            max_retries = job['max_retries']

            # Claim the job
            cursor.execute('''
                UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ?
            ''', (now_str, job_id))
            conn.commit() 
            conn.close()

            print(f"\n[Worker] Processing job '{job_id}' (Attempt {current_attempts + 1}/{max_retries + 1})")
            
            # Run the command and capture its result
            result = subprocess.run(command, shell=True)
            
            # Get fresh timestamps for updating the result
            now = datetime.utcnow()
            now_str = now.isoformat() + "Z"
            
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()

            # CASE A: The command succeeded!
            if result.returncode == 0:
                cursor.execute('''
                    UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?
                ''', (now_str, job_id))
                print(f"[Worker] Job '{job_id}' completed successfully!")
            
            # CASE B: The command failed!
            else:
                new_attempts = current_attempts + 1
                print(f"[Worker] Command failed with exit code {result.returncode}")

                # Check if we have run out of retries
                if new_attempts > max_retries:
                    # Move to DLQ by marking it 'dead'
                    cursor.execute('''
                        UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ? WHERE id = ?
                    ''', (new_attempts, now_str, job_id))
                    print(f"[ERROR] Job '{job_id}' exhausted all retries. Moved to DLQ.")
                
                else:
                    # Calculate exponential backoff delay: base ^ attempts
                    delay_seconds = BACKOFF_BASE ** new_attempts
                    
                    # Calculate exactly when this job is allowed to run again
                    run_after_time = now + timedelta(seconds=delay_seconds)
                    run_after_str = run_after_time.isoformat() + "Z"

                    cursor.execute('''
                        UPDATE jobs SET state = 'failed', attempts = ?, run_after = ?, updated_at = ? 
                        WHERE id = ?
                    ''', (new_attempts, run_after_str, now_str, job_id))
                    print(f"[Worker] Job '{job_id}' failed. Scheduled to retry in {delay_seconds} seconds.")

            conn.commit()
            conn.close()

        except sqlite3.OperationalError:
            time.sleep(0.5)
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(2)

if __name__ == "__main__":
    start_worker()