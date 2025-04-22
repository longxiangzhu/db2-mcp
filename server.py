import os
import subprocess
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

# 设置日志记录 - 只输出到stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
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

# SSE server settings
SSE_PORT = 5555
SSE_HOST = "localhost"

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

# 定义获取tablespace信息的函数
def get_tablespace_info():
    """获取tablespace信息并返回字典数据"""
    global conn
    try:
        # 检查数据库连接并尝试连接
        if not conn:
            connect_result = connect_to_db(DB2_HOSTNAME, DB2_PORT, DB2_USERNAME, DB2_PASSWORD, DB2_DATABASE)
            if connect_result["status"] != "success":
                return {"tablespaces": [], "count": 0, "error": "Database connection failed"}
        
        # 确保我们有一个有效的连接
        if not conn:
            return {"tablespaces": [], "count": 0, "error": "Could not establish database connection"}
        
        # 执行查询
        tablespace_query = "SELECT TBSPACE FROM SYSCAT.TABLESPACES"
        result = execute_sql(tablespace_query)
        
        if result["status"] == "success" and "data" in result:
            # 直接使用TBSPACE键名提取表空间名称
            tablespace_names = [ts["TBSPACE"] for ts in result["data"] if "TBSPACE" in ts]
            
            # 返回字典
            tablespace_info = {
                "tablespaces": tablespace_names,
                "count": len(tablespace_names)
            }
            return tablespace_info
        else:
            error_msg = result.get("message", "Unknown error")
            return {"tablespaces": [], "count": 0, "error": f"Query failed: {error_msg}"}
    except Exception as e:
        return {"tablespaces": [], "count": 0, "error": str(e)}

# 创建MCP服务器实例
def create_mcp_server(transport_type="stdio"):
    global conn
    # Create the FastMCP instance with our system prompt
    system_prompt = """You are an assistant that helps users interact with a DB2 for LUW database.
You can establish connections, run SQL queries, and call stored procedures.
Always provide helpful responses and explain what you're doing."""
    
    logger.info(f"Creating FastMCP instance for {transport_type} transport...")
    
    try:
        # 初始化数据库连接
        conn_result = connect_to_db(DB2_HOSTNAME, DB2_PORT, DB2_USERNAME, DB2_PASSWORD, DB2_DATABASE)
        
        # 创建FastMCP实例，根据传输类型设置配置
        if transport_type == "sse":
            # 为SSE传输设置参数
            mcp = FastMCP(
                system_prompt=system_prompt,
                host=SSE_HOST,
                port=SSE_PORT,
                sse_path="/sse",
                message_path="/messages/"
            )
            logger.info(f"Configured SSE transport on {SSE_HOST}:{SSE_PORT}")
        else:
            # 默认的stdio传输
            mcp = FastMCP(system_prompt=system_prompt)
            logger.info("stdio transport has been configured")
        
        # Add connect_db tool
        mcp.add_tool(
            connect_to_db,
            name="connect_db",
            description="Establish a connection with target DB2 database"
        )
        
        # Add run_sql tool
        mcp.add_tool(
            execute_sql,
            name="run_sql",
            description="Execute a SQL query and return results"
        )
        
        # Add call_sp tool
        mcp.add_tool(
            lambda params: call_stored_procedure(params["sp_name"], *params.get("parameters", [])),
            name="call_sp",
            description="Call a stored procedure or function"
        )
        
        # 添加tablespace资源
        tablespace_resource = FunctionResource(
            uri=AnyUrl("resource://tablespace"),
            name="tablespace",
            description="Tablespace information in the database",
            fn=get_tablespace_info,
            mime_type="application/json"
        )
        mcp.add_resource(tablespace_resource)
        
        # 使用装饰器注册prompt
        @mcp.prompt(name="db2_assistant")
        def db2_prompt():
            return [
                {
                    "role": "user",
                    "content": """You are a helpful assistant that can help users interact with a DB2 for LUW database.
You can establish connections, run SQL queries, and call stored procedures.
Always provide helpful responses and explain what you're doing."""
                }
            ]
        
        return mcp
    except Exception as e:
        logger.error(f"Error creating MCP server: {e}", exc_info=True)
        raise

# 启动SSE服务器进程
def start_sse_server():
    try:
        # 使用当前Python解释器启动一个新进程，运行本脚本并传递sse参数
        sse_process = subprocess.Popen(
            [sys.executable, __file__, "sse"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"Started SSE server process with PID {sse_process.pid}")
        # 记录连接信息
        sse_url = f"http://{SSE_HOST}:{SSE_PORT}/sse"
        message_url = f"http://{SSE_HOST}:{SSE_PORT}/messages/"
        logger.info(f"SSE Connection URL: {sse_url}")
        logger.info(f"SSE Message URL: {message_url}")
        logger.info(f"Connect to this MCP server via SSE at: {sse_url}")
        return sse_process
    except Exception as e:
        logger.error(f"Failed to start SSE server: {e}")
        return None

# 主程序入口
if __name__ == "__main__":
    # 检查是否作为SSE服务器启动
    if len(sys.argv) > 1 and sys.argv[1] == "sse":
        # SSE模式
        try:
            logger.info(f"Starting SSE server on {SSE_HOST}:{SSE_PORT}")
            # 记录SSE连接信息
            sse_url = f"http://{SSE_HOST}:{SSE_PORT}/sse"
            message_url = f"http://{SSE_HOST}:{SSE_PORT}/messages/"
            # logger.info(f"SSE Connection URL: {sse_url}")
            # logger.info(f"SSE Message URL: {message_url}")
            logger.info(f"Connect to this MCP server via SSE at: {sse_url}")
            
            mcp = create_mcp_server("sse")
            mcp.run("sse")
        except Exception as e:
            logger.error(f"Error running SSE server: {e}", exc_info=True)
            sys.exit(1)
    else:
        # 主进程模式：启动SSE服务器子进程，然后以stdio模式运行
        try:
            # 启动SSE服务器子进程
            sse_process = start_sse_server()
            
            # 以stdio模式运行主服务器
            # logger.info("Starting main server with stdio transport")
            mcp = create_mcp_server("stdio")
            mcp.run("stdio")
        except Exception as e:
            logger.error(f"Error running main server: {e}", exc_info=True)
            # 确保退出时关闭子进程
            if 'sse_process' in locals() and sse_process:
                sse_process.terminate()
            sys.exit(1) 