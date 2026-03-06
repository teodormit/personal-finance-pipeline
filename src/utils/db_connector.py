"""
Database Connection Manager - LEAN VERSION
===========================================

PURPOSE: Connect to PostgreSQL database safely and easily

WHAT THIS DOES:
- Opens connections to your PostgreSQL database
- Ensures connections close properly (no memory leaks)
- Loads credentials from .env file (keeps passwords safe)

WHY USE A CLASS?
- Database connection is "stateful" (needs to remember things)
- You want to open connection once, use it many times
- Automatically handles cleanup (closing connections)

"""

import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConnection:
    """
    Simple database connection manager for PostgreSQL
    
    USAGE:
        db = DatabaseConnection()
        
        with db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
  
  
    
    def __init__(self):
        """
        Initialize the database connector
        
        This runs when you do: db = DatabaseConnection()
        
        WHY: Load database credentials from environment variables
        WHEN: Once at the start, before making any connections
        """
        
        # Read credentials from environment variables
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.database = os.getenv('POSTGRES_DB', 'finance_warehouse')
        self.user = os.getenv('POSTGRES_USER', 'teodor_admin')
        self.password = os.getenv('POSTGRES_PASSWORD')
        
        # Store connection string (we'll use this to connect)
        self.connection_string = (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password}"
        )
        
        print(f"Database connector initialized for {self.database}")
    
    # ========================================================================
    # CONCEPT: CONTEXT MANAGER (with statement)
    # ========================================================================
    # Context managers ensure resources are cleaned up properly
    # Like automatically turning off lights when leaving a room
  
  
    
    def connect(self):
        """
        Create a database connection
        
        USAGE:
            with db.connect() as conn:
                # Use the connection here
                # It automatically closes when done
        
        WHY USE 'with':
        - Automatically closes connection (even if error occurs)
        - Prevents memory leaks
        - Makes code cleaner and safer
        
        ANALOGY:
        Opening a file:
            with open('file.txt') as f:
                content = f.read()
            # File automatically closes here
        
        Opening database:
            with db.connect() as conn:
                cursor = conn.cursor()
            # Connection automatically closes here
        """
        
        try:
            # psycopg2.connect() creates the actual connection
            connection = psycopg2.connect(self.connection_string)
            return connection
        
        except psycopg2.Error as e:
            # If connection fails, show helpful error message
            print(f"Failed to connect to database: {str(e)}")
            raise  # Re-raise the error so calling code knows it failed
 
 
    
    def test_connection(self):
        """
        Test if database connection works
        
        USAGE:
            db = DatabaseConnection()
            if db.test_connection():
                print("Ready to load data!")
        
        RETURNS:
            True if connection successful, False otherwise
        """
        
        try:
            # Try to connect and run a simple query
            with self.connect() as conn:
                cursor = conn.cursor()
                
                # Get PostgreSQL version
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"Connected to PostgreSQL")
                print(f" Version: {version[:50]}...")
                
                # Get current database and user
                cursor.execute("SELECT current_database(), current_user;")
                db, user = cursor.fetchone()
                print(f" Database: {db}")
                print(f" User: {user}")
                
                # Check if our schemas exist
                cursor.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('staging', 'bronze', 'silver', 'metadata')
                    ORDER BY schema_name;
                """)
                schemas = [row[0] for row in cursor.fetchall()]
                print(f"Schemas found: {', '.join(schemas)}")
                
                cursor.close()
                return True
        
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            return False


# ============================================================================
# HOW TO USE THIS CLASS
# ============================================================================

if __name__ == "__main__":
    """
    This code runs ONLY when you execute this file directly:
        python src/utils/db_connector.py
    
    It does NOT run when you import this module in other files.
    This is perfect for testing!
    """
    
    print("\n" + "="*60)
    print("DATABASE CONNECTOR TEST")
    print("="*60 + "\n")
    
    # Step 1: Create a database connector object
    # This calls __init__() method
    db = DatabaseConnection()
    
    print()
    
    # Step 2: Test the connection
    if db.test_connection():
        print("\nDatabase connector is working!")
        print("You can now use it to load data.")
    else:
        print("\nDatabase connector test failed.")
        print("Check your .env file and Docker containers.")
    
    print("\n" + "="*60)
