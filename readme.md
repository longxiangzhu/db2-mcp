# DB2 for LUW MCP Server

A Model Context Protocol (MCP) server for interacting with DB2 for LUW databases.

## Features

- Establish connections to DB2 databases
- Execute SQL queries
- Call stored procedures
- Simple client for testing

## Prerequisites

- Python 3.8+
- UV package manager
- DB2 for LUW database

## Setup

1. Clone this repository
2. Configure your database connection in the `.env` file
3. Install dependencies using UV

```bash
# Install UV if you don't have it
curl -sSf https://astral.sh/uv/install.sh | bash

# Create a virtual environment and install dependencies
uv venv
uv pip install -r requirements.txt
```

## Usage

### Starting the server

```bash
python server.py
```

This will start the MCP server on http://localhost:8000.

### Using the client

```bash
python client.py
```

Example commands:
- "Connect to the DB2 database"
- "Run SELECT * FROM SYSCAT.TABLES FETCH FIRST 5 ROWS ONLY"
- "Call the stored procedure 'SAMPLE_SP' with parameters"

## Environment Variables

Configure the following variables in your `.env` file:

```
DB2_DATABASE=your_database
DB2_HOSTNAME=your_hostname
DB2_PORT=your_port
DB2_USERNAME=your_username
DB2_PASSWORD=your_password
```

## MCP Functions

### connect_db
Establishes a connection with the target DB2 database.

### run_sql
Executes a read-only SQL query and returns the results.

### call_sp
Calls a stored procedure or function with the specified parameters.

