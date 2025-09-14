import psycopg2
from psycopg2 import sql, OperationalError
import os
from dotenv import load_dotenv
import sqlparse

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.conn_params = {
            "options":'-c statement_timeout=10s',
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        self.schema = None

    def get_connection(self):
        """Establish a database connection"""
        try:
            return psycopg2.connect(**self.conn_params)
        except OperationalError as e:
            raise Exception(f"Database connection failed: {str(e)}")

    def get_database_schema(self):
        """
        Fetch complete database schema including:
        - Tables and columns
        - Primary keys
        - Foreign keys
        """
        if self.schema:
            return self.schema
        
        schema = []
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    tables = [row[0] for row in cur.fetchall()]

                    for table in tables:
                        cur.execute(sql.SQL("""
                            SELECT 
                                column_name, 
                                data_type,
                                is_nullable,
                                column_default
                            FROM information_schema.columns
                            WHERE table_name = %s
                            ORDER BY ordinal_position
                        """), [table])
                        
                        columns = []
                        for col in cur.fetchall():
                            columns.append({
                                "name": col[0],
                                "type": col[1],
                                "nullable": col[2] == 'YES',
                                "default": col[3]
                            })

                        cur.execute(sql.SQL("""
                            SELECT column_name
                            FROM information_schema.key_column_usage
                            WHERE table_name = %s
                            AND constraint_name IN (
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_name = %s
                                AND constraint_type = 'PRIMARY KEY'
                            )
                        """), [table, table])
                        
                        primary_keys = [row[0] for row in cur.fetchall()]

                        cur.execute(sql.SQL("""
                            SELECT
                                kcu.column_name,
                                ccu.table_name AS foreign_table,
                                ccu.column_name AS foreign_column
                            FROM 
                                information_schema.key_column_usage kcu
                            JOIN information_schema.constraint_column_usage ccu
                                ON ccu.constraint_name = kcu.constraint_name
                            WHERE kcu.table_name = %s
                            AND kcu.constraint_name IN (
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_name = %s
                                AND constraint_type = 'FOREIGN KEY'
                            )
                        """), [table, table])
                        
                        foreign_keys = []
                        for fk in cur.fetchall():
                            foreign_keys.append({
                                "column": fk[0],
                                "references": f"{fk[1]}({fk[2]})"
                            })

                        schema.append({
                            "name": table,
                            "columns": columns,
                            "primary_keys": primary_keys,
                            "foreign_keys": foreign_keys
                        })

                    self.schema = schema
                    return schema

        except Exception as e:
            return {"error": str(e)}

    def execute_sql_query(self, sql_query):
        """
        Execute SQL query and return results with metadata
        Handles SELECT queries
        """
        def is_safe_sql(sql_query):
            return sqlparse.parse(sql_query)[0].get_type() == "SELECT"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Handle SELECT queries
                    if is_safe_sql(sql_query):
                        cur.execute(sql_query)
                        columns = [desc[0] for desc in cur.description]
                        data = cur.fetchall()
                        return {
                            "columns": columns,
                            "data": data,
                            "row_count": len(data)
                        }
                    else:
                        return {"error": "Query not supported."}

        except psycopg2.Error as e:
            return {"error": str(e)}

    def get_formatted_schema(self, filtered_tables=None):
        """Return database_schema as formatted string"""
        schema = self.get_database_schema()
        schema_text = []
        for table in schema:
            if filtered_tables is None or table['name'] in filtered_tables:
                schema_text.append(f"Table: {table['name']}")
                
                # Columns
                columns = []
                for column in table["columns"]:
                    column_def = f"{column['name']} ({column['type']})"
                    columns.append(column_def)
                schema_text.append("  Columns: " + ", ".join(columns))
                
                # Primary keys
                if table["primary_keys"]:
                    schema_text.append(f"  Primary Key: {', '.join(table['primary_keys'])}")
                
                # Foreign keys
                for fk in table["foreign_keys"]:
                    schema_text.append(f"  Foreign Key: {fk['column']} references {fk['references']}")
                
                schema_text.append("")  
        
        return "\n".join(schema_text)
    
    def get_normalized_create_statement(self, filtered_tables=None):
        """Return database_schema as PostgreSQL normalized create table statements"""
        schema = self.get_database_schema()
        schema_text = []
        for table in schema:
            if filtered_tables is None or table['name'] in filtered_tables:
                create_table_text = f"create table {table['name']} ("
                
                # Columns
                columns = []
                for column in table["columns"]:
                    column_def = f"{column['name']} {column['type'].lower()}"
                    columns.append(column_def)
                create_table_text += ("\n" + ",\n".join(columns))
                
                # Primary keys
                if table["primary_keys"]:
                    create_table_text += (f",\nprimary key ({', '.join(table['primary_keys'])})")
                
                # Foreign keys
                for fk in table["foreign_keys"]:
                    create_table_text += (f",\nforeign key ({fk['column']}) references {fk['references']}")

                create_table_text += "\n);"

                schema_text.append(create_table_text)  
        
        return "\n".join(schema_text)

# Singleton instance for the application
db_manager = DatabaseManager()

# Interface functions for FastAPI
def get_database_schema(filtered_tables=None):
    """Public interface for schema retrieval"""
    return db_manager.get_formatted_schema(filtered_tables)

def get_normalized_create_statement(filtered_tables=None):
    """Public interface for schema retrieval"""
    return db_manager.get_normalized_create_statement(filtered_tables)

def execute_sql_query(sql_query):
    """Public interface for query execution"""
    return db_manager.execute_sql_query(sql_query)