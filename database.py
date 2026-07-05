import sqlite3


DB_FILE = "queue.db"

def init_db():
    print("Connecting to the database...")
   
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("Creating the 'jobs' table...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            run_after TEXT
        )
    ''')

    conn.commit()
    conn.close()
    print("Database setup complete! The 'jobs' table is ready.")
if __name__ == "__main__":
    init_db()   