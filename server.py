import os
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
import ibm_db
import json
import logging
import sys
from pathlib import Path
from mcp.server import FastMCP
from mcp.server.fastmcp.resources import FunctionResource
from pydantic import AnyUrl

# 创建日志目录在当前工作目录下
current_dir = Path(os.getcwd())
log_dir = current_dir / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "db2_mcp.log"

# 设置日志记录 - 确保同时输出到控制台和文件
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stderr)  # 使用stderr而不是stdout
    ]
)
logger = logging.getLogger("db2-mcp")

# 输出初始调试信息
print(f"Debug: Log file will be written to {log_file}", file=sys.stderr)
logger.info(f"Starting DB2 MCP - Logs will be written to {log_file}")

# Load environment variables from .env file
load_dotenv()

# Get database connection parameters from environment variables
DB2_HOSTNAME = os.getenv("DB2_HOSTNAME")
DB2_PORT = os.getenv("DB2_PORT")
DB2_DATABASE = os.getenv("DB2_DATABASE")
DB2_USERNAME = os.getenv("DB2_USERNAME")
DB2_PASSWORD = os.getenv("DB2_PASSWORD")

logger.info(f"Database config: Host={DB2_HOSTNAME}, Port={DB2_PORT}, DB={DB2_DATABASE}")

# Global connection variable
conn = None

# Function to establish database connection
def connect_to_db(ip: str, port: str, username: str, password: str, dbname: str):
    global conn
    try:
        # Create the connection string
        conn_string = f"DATABASE={dbname};HOSTNAME={ip};PORT={port};PROTOCOL=TCPIP;UID={username};PWD={password};"
        logger.debug(f"Attempting connection with: DATABASE={dbname};HOSTNAME={ip};PORT={port};PROTOCOL=TCPIP;UID={username};PWD=***")
        
        # Establish the connection
        conn = ibm_db.connect(conn_string, "", "")
        
        # Test the connection
        if conn:
            logger.info(f"Successfully connected to DB2 database {dbname} on {ip}:{port}")
            return {"status": "success", "message": f"Successfully connected to DB2 database {dbname} on {ip}:{port}"}
        else:
            logger.error("Failed to connect to DB2 database - no error but conn is None")
            return {"status": "error", "message": "Failed to connect to DB2 database"}
    except Exception as e:
        # If connection fails, return error
        logger.exception(f"Connection error: {str(e)}")
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

# 定义获取tablespace信息的函数
def get_tablespace_info():
    """获取tablespace信息并返回字典数据(而不是JSON字符串)"""
    global conn
    logger.debug("get_tablespace_info called")
    try:
        # 检查数据库连接并尝试连接
        if not conn:
            logger.warning("Database connection not established in resource request. Connecting...")
            connect_result = connect_to_db(DB2_HOSTNAME, DB2_PORT, DB2_USERNAME, DB2_PASSWORD, DB2_DATABASE)
            logger.info(f"Resource request connection result: {connect_result}")
            if connect_result["status"] != "success":
                logger.error(f"Failed to connect to database in resource request: {connect_result}")
                return {"tablespaces": [], "count": 0, "error": "Database connection failed"}
        
        # 确保我们有一个有效的连接
        if not conn:
            logger.error("Connection still not established after connection attempt")
            return {"tablespaces": [], "count": 0, "error": "Could not establish database connection"}
        
        # 执行查询
        tablespace_query = "SELECT TBSPACE FROM SYSCAT.TABLESPACES"
        logger.info(f"Executing query in resource request: {tablespace_query}")
        result = execute_sql(tablespace_query)
        logger.info(f"Resource request query result status: {result['status']}")
        
        # 添加更多调试信息
        if "message" in result:
            logger.debug(f"Query message: {result['message']}")
        
        if result["status"] == "success" and "data" in result:
            data_count = len(result["data"])
            logger.info(f"Found {data_count} tablespaces in resource request")
            
            # 打印结果中的第一条记录的结构和键名
            if data_count > 0:
                sample_record = result["data"][0]
                logger.info(f"Resource request sample record keys: {list(sample_record.keys())}")
                logger.info(f"Resource request sample record: {sample_record}")
            else:
                logger.warning("Query returned empty result set")
                return {"tablespaces": [], "count": 0, "message": "No tablespaces found"}
            
            # 使用大写列名尝试获取
            key_name = "TBSPACE"
            if key_name not in sample_record:
                # 尝试其他可能的列名
                possible_keys = ["tbspace", "TBSPACE_NAME", "tbspace_name", "tablespace", "TABLESPACE"]
                key_found = False
                for pk in possible_keys:
                    if pk in sample_record:
                        key_name = pk
                        key_found = True
                        logger.info(f"Found alternative key name in resource request: {key_name}")
                        break
                
                if not key_found:
                    logger.error(f"Could not find tablespace key in result. Available keys: {list(sample_record.keys())}")
                    # 返回所有数据让用户查看
                    return {
                        "error": "Could not find tablespace key",
                        "available_keys": list(sample_record.keys()),
                        "sample_record": sample_record,
                        "tablespaces": [],
                        "count": 0
                    }
            
            # 提取表空间名称
            tablespace_names = [ts[key_name] for ts in result["data"] if key_name in ts]
            logger.info(f"Extracted tablespace names in resource request: {tablespace_names}")
            
            # 返回字典(不是JSON字符串)
            tablespace_info = {
                "tablespaces": tablespace_names,
                "count": len(tablespace_names)
            }
            return tablespace_info
        else:
            error_msg = result.get("message", "Unknown error")
            logger.error(f"Query failed or no data returned in resource request: {error_msg}")
            return {"tablespaces": [], "count": 0, "error": f"Query failed: {error_msg}"}
    except Exception as e:
        logger.exception(f"Error retrieving tablespace info in resource request: {str(e)}")
        return {"tablespaces": [], "count": 0, "error": str(e)}

# 创建MCP服务器实例
def create_mcp_server():
    global conn
    # Create the FastMCP instance with our system prompt
    system_prompt = """You are an assistant that helps users interact with a DB2 for LUW database.
You can establish connections, run SQL queries, and call stored procedures.
Always provide helpful responses and explain what you're doing."""
    
    logger.info("Creating FastMCP instance...")
    
    try:
        # 初始化数据库连接
        logger.info("Establishing initial database connection...")
        conn_result = connect_to_db(DB2_HOSTNAME, DB2_PORT, DB2_USERNAME, DB2_PASSWORD, DB2_DATABASE)
        logger.info(f"Initial database connection result: {conn_result}")
        
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
        
        # 添加tablespace资源，使用FunctionResource确保每次读取都重新查询最新数据
        tablespace_resource = FunctionResource(
            uri=AnyUrl("resource://tablespace"),
            name="tablespace",
            description="Tablespace information in the database",
            fn=get_tablespace_info,  # 直接使用函数，FunctionResource内部会处理JSON序列化
            mime_type="application/json"
        )
        mcp.add_resource(tablespace_resource)
        logger.info("Added tablespace resource")
        
        # 测试获取tablespace信息
        test_info = get_tablespace_info()
        logger.info(f"Test tablespace info: {json.dumps(test_info, indent=2)}")
        
        # Print all available tools
        logger.info(f"Available tools: {mcp.list_tools()}")
        
        return mcp
    except Exception as e:
        logger.error(f"Error creating MCP server: {e}", exc_info=True)
        raise

# 主程序入口
if __name__ == "__main__":
    try:
        mcp = create_mcp_server()
        logger.info("Server is starting...")
        mcp.run()
    except Exception as e:
        logger.error(f"Error running server: {e}", exc_info=True)
        sys.exit(1) 