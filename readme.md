# DB2 for LUW MCP Server

A Model Context Protocol (MCP) server for interacting with DB2 for LUW databases.

## Features

- Establish connections to DB2 databases
- Execute SQL queries
- Call stored procedures


## Setup

1. Clone this repository
2. Configure your database connection in the `.env` file
3. Install dependencies 

```python
python3 -m venv db2_x86_env

source db2_x86_env/bin/activate && pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org ibm-db python-dotenv==1.0.0 "mcp @ git+https://github.com/modelcontextprotocol/python-sdk.git"

```

## Usage

### Starting the server

```
source ./db2_x86_env/bin/activate && python server.py

```

## Environment Variables

Configure the following variables in your `.env` file:

```
DB2_DATABASE=your_database
DB2_HOSTNAME=your_hostname
DB2_PORT=your_port
DB2_USERNAME=your_username
DB2_PASSWORD=your_password
```

## MCP config for Cursor or cherry studio 

### Cursor
```json
{
  "mcpServers": {
    "db2-mcp": {
      "autoApprove": [
        "connect_db"
      ],
      "disabled": false,
      "timeout": 60,
      "command": "/Users/zlx/Desktop/zlx/3.Coding/AI/db2-mcp/python_x86_wrapper.sh",
      "args": [
        "/Users/zlx/Desktop/zlx/3.Coding/AI/db2-mcp/server.py"
      ],
      "env": {
        "DB2_DATABASE": "tpcc",
        "DB2_HOSTNAME": "192.168.0.100",
        "DB2_PORT": "50000",
        "DB2_USERNAME": "db2user",
        "DB2_PASSWORD": "db2user@2025"
      },
      "transportType": "stdio"
    }
  }
}
```

### Cherry Studio 

```json
  "ECvXtBighSOVs1JMd1GBy": {
      "name": "db2-mcp",
      "type": "stdio",
      "description": "",
      "isActive": true,
      "command": "/Users/zlx/Desktop/zlx/3.Coding/AI/db2-mcp/python_x86_wrapper.sh",
      "args": [
        "/Users/zlx/Desktop/zlx/3.Coding/AI/db2-mcp/server.py"
      ],
      "env": {
        "DB2_DATABASE": "tpcc",
        "DB2_HOSTNAME": "192.168.0.100",
        "DB2_PORT": "50000",
        "DB2_USERNAME": "db2user",
        "DB2_PASSWORD": "db2user@2025"
      },
    }
```

## MCP Functions

### connect_db
Establishes a connection with the target DB2 database.

### run_sql
Executes a read-only SQL query and returns the results.

### call_sp
Calls a stored procedure or function with the specified parameters.
