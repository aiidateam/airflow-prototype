#!/usr/bin/env python3
"""
Utility script to check the contents of the DagRun tracking SQLite database.
"""
import sqlite3
import os
from tabulate import tabulate

def check_database():
    """Check and display the contents of the DagRun tracking database."""
    db_path = os.path.join(os.path.dirname(__file__), 'dagrun_tracking.db')

    if not os.path.exists(db_path):
        print(f"Database not found at: {db_path}")
        print("The database will be created when the first DAG run event occurs.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Get table info
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='dagrun_events'")
            table_schema = cursor.fetchone()
            if table_schema:
                print("Table Schema:")
                print(table_schema[0])
                print("\n" + "="*80 + "\n")

            # Get all records
            cursor.execute('''
                SELECT id, dag_id, run_id, state, event_type, event_timestamp, created_at
                FROM dagrun_events
                ORDER BY created_at DESC
            ''')
            records = cursor.fetchall()

            if records:
                headers = ['ID', 'DAG ID', 'Run ID', 'State', 'Event Type', 'Event Time', 'Created At']
                print("DagRun Events:")
                print(tabulate(records, headers=headers, tablefmt='grid'))
                print(f"\nTotal records: {len(records)}")
            else:
                print("No DagRun events found in the database.")

            # Get summary statistics
            cursor.execute('''
                SELECT event_type, COUNT(*) as count
                FROM dagrun_events
                GROUP BY event_type
                ORDER BY count DESC
            ''')
            stats = cursor.fetchall()

            if stats:
                print("\nEvent Type Summary:")
                print(tabulate(stats, headers=['Event Type', 'Count'], tablefmt='grid'))

    except Exception as e:
        print(f"Error reading database: {e}")

if __name__ == "__main__":
    check_database()