import argparse
import json
import sqlite3
import uuid
from datetime import datetime

DB_FILE = "queue.db"
CONFIG_FILE = "config.json"

def get_max_retries_config():
    """Reads the current max_retries configuration from config.json."""
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            return config.get("max_retries", 3)
    except FileNotFoundError:
        return 3 # Default fallback

# ==========================================
# COMMAND 1: ENQUEUE JOB
# ==========================================
def enqueue_job(job_json_string):
    try:
        job_data = json.loads(job_json_string)
    except json.JSONDecodeError:
        print("Error: The data provided is not valid JSON.")
        return

    job_id = job_data.get("id", str(uuid.uuid4()))
    command = job_data.get("command")
    
    if not command:
        print("Error: You must provide a 'command' to run.")
        return

    state = "pending"
    attempts = 0
    # Read dynamically from config file to avoid hardcoding!
    max_retries = get_max_retries_config() 
    
    now = datetime.utcnow().isoformat() + "Z"

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, run_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_id, command, state, attempts, max_retries, now, now, now))
        conn.commit()
        conn.close()
        print(f"Successfully enqueued job: {job_id} (Max Retries Configured: {max_retries})")
    except sqlite3.IntegrityError:
        print(f"Error: A job with the ID '{job_id}' already exists!")

# ==========================================
# COMMAND 2: STATUS SUMMARY
# ==========================================
def show_status():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # SQL query to count jobs grouped by their current state
    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    rows = cursor.fetchall()
    conn.close()

    status_dict = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
    for state, count in rows:
        if state in status_dict:
            status_dict[state] = count

    print("\nState          | Count")
    print("-------------------------")
    for state, count in status_dict.items():
        print(f"{state:<14} | {count}")
    print("")

# ==========================================
# COMMAND 3: LIST JOBS BY STATE
# ==========================================
def list_state(state_name):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, command, attempts, updated_at FROM jobs WHERE state = ?", (state_name,))
    jobs = cursor.fetchall()
    conn.close()

    if not jobs:
        print(f"No jobs found with state '{state_name}'.")
        return

    print(f"\n--- Jobs in state: {state_name} ---")
    for job in jobs:
        print(f"ID: {job['id']} | Command: '{job['command']}' | Attempts: {job['attempts']} | Last Updated: {job['updated_at']}")
    print("")

# ==========================================
# COMMAND 4: DLQ ACTIONS (LIST / RETRY)
# ==========================================
def handle_dlq(action, job_id=None):
    conn = sqlite3.connect(DB_FILE)
    
    if action == "list":
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, command, attempts, updated_at FROM jobs WHERE state = 'dead'")
        dead_jobs = cursor.fetchall()
        
        if not dead_jobs:
            print("The Dead Letter Queue (DLQ) is empty.")
        else:
            print("\n--- Dead Letter Queue (DLQ) ---")
            for job in dead_jobs:
                print(f"ID: {job['id']} | Failed Command: '{job['command']}' | Total Attempts: {job['attempts']}")
            print("")
            
    elif action == "retry":
        if not job_id:
            print("Error: You must specify a job ID to retry. Example: queuectl dlq retry job1")
            conn.close()
            return
            
        cursor = conn.cursor()
        # Find if the job exists and is actually dead
        cursor.execute("SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
        job = cursor.fetchone()
        
        if not job:
            print(f"Error: No dead job found with ID '{job_id}'.")
        else:
            now = datetime.utcnow().isoformat() + "Z"
            # Reset attempts to 0, move back to pending, and clear run_after timeline
            cursor.execute('''
                UPDATE jobs 
                SET state = 'pending', attempts = 0, updated_at = ?, run_after = ? 
                WHERE id = ?
            ''', (now, now, job_id))
            conn.commit()
            print(f"Successfully resurrected job '{job_id}' from DLQ! Moved back to pending.")
            
    conn.close()

# ==========================================
# COMMAND 5: CONFIG SET
# ==========================================
def set_config(key, value):
    if key != "max-retries":
        print(f"Error: Unknown configuration key '{key}'. Supported keys: max-retries")
        return
        
    try:
        int_value = int(value)
    except ValueError:
        print("Error: max-retries value must be a valid integer.")
        return

    # Update config.json file
    config_data = {"max_retries": int_value}
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_data, f, indent=4)
        
    print(f"Configuration updated successfully! Global '{key}' is now set to {int_value}.")

# ==========================================
# MAIN CLI PARSER ENGINE
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="QueueCTL - Background Job Queue System")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. Enqueue Subparser
    enqueue_parser = subparsers.add_parser("enqueue", help="Add a new job to the queue")
    enqueue_parser.add_argument("job_data", help="JSON string representing the job")

    # 2. Status Subparser
    subparsers.add_parser("status", help="Show summary of all job states")

    # 3. List-state Subparser
    list_parser = subparsers.add_parser("list-state", help="List jobs filtered by state")
    list_parser.add_argument("state_name", choices=["pending", "processing", "completed", "failed", "dead"], help="State name")

    # 4. DLQ Subparser
    dlq_parser = subparsers.add_parser("dlq", help="View or retry dead jobs")
    dlq_parser.add_argument("action", choices=["list", "retry"], help="Action to perform on DLQ")
    dlq_parser.add_argument("job_id", nargs="?", default=None, help="Job ID (Only required for retry action)")

    # 5. Config Subparser
    config_parser = subparsers.add_parser("config", help="Manage application configuration")
    config_parser.add_argument("action", choices=["set"], help="Configuration action")
    config_parser.add_argument("key", help="Configuration key (e.g., max-retries)")
    config_parser.add_argument("value", help="Value to assign to the key")

    args = parser.parse_args()

    if args.command == "enqueue":
        enqueue_job(args.job_data)
    elif args.command == "status":
        show_status()
    elif args.command == "list-state":
        list_state(args.state_name)
    elif args.command == "dlq":
        handle_dlq(args.action, args.job_id)
    elif args.command == "config":
        if args.action == "set":
            set_config(args.key, args.value)

if __name__ == "__main__":
    main()