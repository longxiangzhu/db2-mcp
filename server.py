import os
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
import ibm_db
import json
import logging
import sys
from mcp.server import FastMCP

# 设置日志记录
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("db2-mcp")

# Load environment variables from .env file
load_dotenv()

# Get database connection parameters from environment variables
DB2_HOSTNAME = os.getenv("DB2_HOSTNAME")
DB2_PORT = os.getenv("DB2_PORT")
DB2_DATABASE = os.getenv("DB2_DATABASE")
DB2_USERNAME = os.getenv("DB2_USERNAME")
DB2_PASSWORD = os.getenv("DB2_PASSWORD")

# Global connection variable
conn = None

# Function to establish database connection
def connect_to_db(ip: str, port: str, username: str, password: str, dbname: str):
    global conn
    try:
        # Create the connection string
        conn_string = f"DATABASE={dbname};HOSTNAME={ip};PORT={port};PROTOCOL=TCPIP;UID={username};PWD={password};"
        
        # Establish the connection
        conn = ibm_db.connect(conn_string, "", "")
        
        # Test the connection
        if conn:
            return {"status": "success", "message": f"Successfully connected to DB2 database {dbname} on {ip}:{port}"}
        else:
            return {"status": "error", "message": "Failed to connect to DB2 database"}
    except Exception as e:
        # If connection fails, return error
        return {"status": "error", "message": f"Connection error: {str(e)}"}

# Function to execute a SQL query
def execute_sql(sql: str):
    global conn
    if not conn:
        return {"status": "error", "message": "Database connection not established. Use connect_db first."}
    
    try:
        # Execute the SQL statement
        stmt = ibm_db.exec_immediate(conn, sql)
        
        # If it's a SELECT statement, fetch the results
        if sql.strip().upper().startswith("SELECT"):
            # Initialize result list
            result = []
            
            # Get column info
            columns = []
            col_num = ibm_db.num_fields(stmt)
            for i in range(col_num):
                col_name = ibm_db.field_name(stmt, i)
                columns.append(col_name)
            
            # Fetch rows and add to result
            row = ibm_db.fetch_assoc(stmt)
            while row:
                result.append(row)
                row = ibm_db.fetch_assoc(stmt)
            
            return {
                "status": "success",
                "message": f"SQL query executed successfully",
                "data": result,
                "columns": columns
            }
        else:
            # For non-SELECT statements
            return {
                "status": "success",
                "message": f"SQL statement executed successfully"
            }
    except Exception as e:
        return {"status": "error", "message": f"SQL execution error: {str(e)}"}

# Function to call stored procedures
def call_stored_procedure(sp_name: str, *args):
    global conn
    if not conn:
        return {"status": "error", "message": "Database connection not established. Use connect_db first."}
    
    try:
        # Create the CALL statement
        if args and len(args) > 0:
            # Format parameters for the stored procedure call
            params = ','.join(['?' for _ in args])
            call_stmt = f"CALL {sp_name}({params})"
        else:
            call_stmt = f"CALL {sp_name}()"
        
        # Prepare the statement
        stmt = ibm_db.prepare(conn, call_stmt)
        
        # Bind parameters if any
        for i, arg in enumerate(args):
            ibm_db.bind_param(stmt, i + 1, arg)
        
        # Execute the statement
        result = ibm_db.execute(stmt)
        
        if result:
            # Initialize result list
            data = []
            
            # Try to fetch results if available
            try:
                # Get column info
                columns = []
                col_num = ibm_db.num_fields(stmt)
                for i in range(col_num):
                    col_name = ibm_db.field_name(stmt, i)
                    columns.append(col_name)
                
                # Fetch rows and add to result
                row = ibm_db.fetch_assoc(stmt)
                while row:
                    data.append(row)
                    row = ibm_db.fetch_assoc(stmt)
                
                return {
                    "status": "success",
                    "message": f"Stored procedure {sp_name} executed successfully",
                    "data": data,
                    "columns": columns
                }
            except:
                # If no result set available
                return {
                    "status": "success",
                    "message": f"Stored procedure {sp_name} executed successfully",
                    "data": []
                }
        else:
            return {
                "status": "warning",
                "message": f"Stored procedure {sp_name} called, but returned no results or errors"
            }
    except Exception as e:
        return {"status": "error", "message": f"Stored procedure execution error: {str(e)}"}

# Start the server
if __name__ == "__main__":
    print("Starting DB2 MCP Server (Real DB2 Connection Mode)...")
    print(f"Using database: {DB2_DATABASE} on {DB2_HOSTNAME}:{DB2_PORT}")
    
    # Create the FastMCP instance with our system prompt
    system_prompt = """You are an assistant that helps users interact with a DB2 for LUW database.
You can establish connections, run SQL queries, and call stored procedures.
Always provide helpful responses and explain what you're doing."""
    
    logger.info("Creating FastMCP instance...")
    
    try:
        # Create the FastMCP instance without tools initially
        mcp = FastMCP(system_prompt=system_prompt)
        logger.info("FastMCP instance created successfully")
        
        # Add each tool using the FastMCP add_tool method
        # Note: We're passing functions directly instead of Tool objects
        
        # Add connect_db tool
        mcp.add_tool(
            connect_to_db,
            name="connect_db",
            description="Establish a connection with target DB2 database"
        )
        logger.info("Added connect_db tool")
        
        # Add run_sql tool
        mcp.add_tool(
            execute_sql,
            name="run_sql",
            description="Execute a SQL query and return results"
        )
        logger.info("Added run_sql tool")
        
        # Add call_sp tool
        mcp.add_tool(
            lambda params: call_stored_procedure(params["sp_name"], *params.get("parameters", [])),
            name="call_sp",
            description="Call a stored procedure or function"
        )
        logger.info("Added call_sp tool")
        
        # Print all available tools
        logger.info(f"Available tools: {mcp.list_tools()}")
        
        # Start the server
        print("Server is running. You can interact with it using an MCP client.")
        logger.info("Starting server...")
        mcp.run()
    except Exception as e:
        logger.error(f"Error starting server: {e}", exc_info=True) 