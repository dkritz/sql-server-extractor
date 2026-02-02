# SQL Server Object Extractor

A Python script that extracts table DDLs, view definitions, and stored procedures from SQL Server and organizes them in a hierarchical folder structure.

## Features

- Extracts table DDLs, view definitions, and stored procedures
- Discovers all user databases automatically
- Organizes output in folder structure: `server/database/object_type/`
- Supports configuration file for credentials
- Comprehensive logging and error handling
- Generates extraction report

## Prerequisites

- Python 3.6+
- ODBC Driver 17 for SQL Server
- pyodbc package

## Installation

1. Install ODBC Driver 17 for SQL Server:
   - Windows: Download from Microsoft website
   - Linux: `curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -`
   - macOS: `brew install unixodbc`

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Arguments

```bash
python sql_server_extractor.py --server SERVER_NAME --username USER --password PASS [OPTIONS]
```

#### Required Arguments
- `--server`: SQL Server hostname or IP address
- `--username`: SQL Server login username
- `--password`: SQL Server login password

#### Optional Arguments
- `--port`: SQL Server port (default: 1433)
- `--output`: Output directory (default: sql_extracted_objects)
- `--config`: Configuration file path (default: config.json)
- `--no-trust-cert`: Don't trust server certificate

### Configuration File

Create a `config.json` file:

```json
{
  "server": "your_server_name",
  "username": "your_username", 
  "password": "your_password",
  "port": 1433,
  "output_dir": "sql_extracted_objects",
  "trust_cert": true
}
```

Then run:

```bash
python sql_server_extractor.py --config config.json
```

## Output Structure

The script creates the following folder structure:

```
sql_extracted_objects/
├── SERVER_NAME/
│   ├── tables/
│   │   ├── Database1/
│   │   │   ├── dbo.Table1.sql
│   │   │   └── dbo.Table2.sql
│   │   └── Database2/
│   │       └── dbo.Table3.sql
│   ├── views/
│   │   ├── Database1/
│   │   │   └── dbo.View1.sql
│   │   └── Database2/
│   │       └── dbo.View2.sql
│   └── stored_procedures/
│       ├── Database1/
│       │   ├── dbo.Proc1.sql
│       │   └── dbo.Proc2.sql
│       └── Database2/
│           └── dbo.Proc3.sql
└── extraction_report.json
```

## Example Files

Each extracted file contains:
- Extraction timestamp
- Database and object information
- Full object definition

```sql
-- Extracted on 2024-01-15T10:30:00.000000
-- Database: MyDatabase
-- Object: dbo.Customers

CREATE TABLE [dbo].[Customers] (
    [CustomerID] int IDENTITY(1,1) NOT NULL,
    [Name] varchar(100) NOT NULL,
    [Email] varchar(255) NULL,
    [CreatedDate] datetime NOT NULL
);
```

## Logging

The script creates a `sql_extractor.log` file with detailed logging information including:
- Connection status
- Database discovery
- Object counts
- Errors and warnings
- Progress updates

## Security Notes

- Store credentials securely (environment variables, vault systems)
- Use SQL Server authentication with limited privileges
- Review extracted code for sensitive information
- Consider using Windows Authentication where possible

## Troubleshooting

### Connection Issues
- Ensure ODBC Driver 17 is installed
- Verify server name and port
- Check firewall settings
- Validate credentials

### Permission Issues
- User needs `VIEW DEFINITION` permission on objects
- May need `db_datareader` for certain operations
- Test with SQL Server Management Studio first

### Extraction Issues
- Check log file for detailed error messages
- Some system objects are excluded by design
- Encrypted objects cannot be extracted

## License

This script is provided as-is for educational and development purposes.