import os
import psycopg2
from datetime import datetime

# ============================================================
# DATA ENGINEERING LAB — F1 CRUD Demo (Phase 13)
# ============================================================

def get_db():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )

def run_crud_demo():
    print("🏎️ Starting F1 CRUD & Atomicity Demo...")
    conn = get_db()
    
    try:
        with conn.cursor() as cur:
            # Demonstration of Atomicity (BEGIN transaction)
            print("\n--- BEGIN TRANSACTION ---")
            
            # 1. INSERT (New Driver)
            print("1. INSERT: Adding a new driver (demo_driver)...")
            cur.execute("""
                INSERT INTO warehouse.dim_driver (driver_id, driver_name, code, nationality)
                VALUES ('demo_driver', 'John Doe', 'DOE', 'American')
                ON CONFLICT (driver_id) DO NOTHING;
            """)
            
            cur.execute("SELECT * FROM warehouse.dim_driver WHERE driver_id = 'demo_driver'")
            print("   -> Inserted:", cur.fetchone())

            # 2. UPDATE (Modify Driver)
            print("\n2. UPDATE: Modifying driver nationality and code...")
            cur.execute("""
                UPDATE warehouse.dim_driver 
                SET nationality = 'Canadian', code = 'JDO'
                WHERE driver_id = 'demo_driver'
            """)
            
            cur.execute("SELECT * FROM warehouse.dim_driver WHERE driver_id = 'demo_driver'")
            print("   -> Updated:", cur.fetchone())

            # 3. DELETE (Remove Driver)
            print("\n3. DELETE: Removing the demo driver...")
            cur.execute("DELETE FROM warehouse.dim_driver WHERE driver_id = 'demo_driver'")
            
            cur.execute("SELECT * FROM warehouse.dim_driver WHERE driver_id = 'demo_driver'")
            result = cur.fetchone()
            print(f"   -> Deleted? (None expected): {result}")
            
            # Commit the atomic transaction
            conn.commit()
            print("\n--- COMMIT SUCCESSFUL ---")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error occurred. Transaction ROLLED BACK. {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_crud_demo()
