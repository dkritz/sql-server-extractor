#!/usr/bin/env python3
"""
SQL Server Object Extractor
Extracts table DDLs, view definitions, and stored procedures from SQL Server
and organizes them in a folder structure: server/database/object_type/
"""

import pyodbc
import os
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator, Union, Literal


class ObjectType(Enum):
    TABLE = "tables"
    VIEW = "views"
    PROCEDURE = "stored_procedures"


@dataclass
class DatabaseObject:
    schema: str
    name: str
    
    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass
class TableInfo(DatabaseObject):
    pass


@dataclass
class ViewInfo(DatabaseObject):
    pass


@dataclass
class ProcedureInfo(DatabaseObject):
    pass


class SQLExtractorError(Exception):
    """Base exception for SQL extractor operations"""
    pass


class ConnectionError(SQLExtractorError):
    """Exception raised when database connection fails"""
    pass


class DDLExtractionError(SQLExtractorError):
    """Exception raised when DDL extraction fails"""
    pass


class SQLServerExtractor:
    def __init__(self, server: str, username: str, password: str, 
                 output_dir: str = "sql_extracted_objects", port: int = 1433,
                 trust_cert: bool = True):
        self.server = server
        self.username = username
        self.password = password
        self.port = port
        self.trust_cert = trust_cert
        self.output_dir = Path(output_dir)
        self._connection: Optional[pyodbc.Connection] = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sql_extractor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    @property
    def connection_string(self) -> str:
        """Generate database connection string"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"UID={self.username};"
            f"PWD={self.password};"
        )
        if self.trust_cert:
            conn_str += "TrustServerCertificate=yes;"
        return conn_str
    
    @contextmanager
    def get_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager for database connections"""
        try:
            self.logger.info(f"Connecting to SQL Server: {self.server}")
            self._connection = pyodbc.connect(self.connection_string, timeout=30)
            self.logger.info("Successfully connected to SQL Server")
            yield self._connection
        except pyodbc.Error as e:
            self.logger.error(f"Failed to connect to SQL Server: {e}")
            raise ConnectionError(f"Database connection failed: {e}") from e
        finally:
            if self._connection:
                self._connection.close()
                self._connection = None
    
    @property
    def connection(self) -> Optional[pyodbc.Connection]:
        """Get current database connection"""
        return self._connection
    
    def connect(self) -> bool:
        """Establish connection to SQL Server (legacy method)"""
        try:
            self._connection = pyodbc.connect(self.connection_string, timeout=30)
            self.logger.info("Successfully connected to SQL Server")
            return True
        except pyodbc.Error as e:
            self.logger.error(f"Failed to connect to SQL Server: {e}")
            return False
    
    def get_databases(self) -> List[str]:
        """Get list of databases excluding system databases"""
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        query = """
        SELECT name FROM sys.databases 
        WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb')
        AND state = 0
        ORDER BY name
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        databases = [row[0] for row in cursor.fetchall()]
        self.logger.info(f"Found {len(databases)} user databases")
        return databases
    
    def get_tables(self, database: str) -> List[TableInfo]:
        """Get list of user tables in a database"""
        query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM [{database}].INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        tables = [TableInfo(schema=row[0], name=row[1]) for row in cursor.fetchall()]
        self.logger.info(f"Found {len(tables)} tables in {database}")
        return tables
    
    def get_views(self, database: str) -> List[ViewInfo]:
        """Get list of views in a database"""
        query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM [{database}].INFORMATION_SCHEMA.VIEWS
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        views = [ViewInfo(schema=row[0], name=row[1]) for row in cursor.fetchall()]
        self.logger.info(f"Found {len(views)} views in {database}")
        return views
    
    def get_stored_procedures(self, database: str) -> List[ProcedureInfo]:
        """Get list of stored procedures in a database"""
        query = f"""
        SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME
        FROM [{database}].INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        procedures = [ProcedureInfo(schema=row[0], name=row[1]) for row in cursor.fetchall()]
        self.logger.info(f"Found {len(procedures)} stored procedures in {database}")
        return procedures
    
    def get_table_ddl(self, database: str, schema: str, table: str) -> str:
        """Extract table DDL using sp_help"""
        try:
            cursor = self.connection.cursor()
            
            # Get basic table info
            cursor.execute(f"USE [{database}]")
            cursor.execute(f"""
            SELECT 
                obj.name AS table_name,
                sc.name AS schema_name,
                CONVERT(varchar(max), 
                    OBJECT_DEFINITION(obj.object_id)) as definition
            FROM sys.objects obj
            JOIN sys.schemas sc ON obj.schema_id = sc.schema_id
            WHERE obj.type = 'U' 
            AND obj.name = ? 
            AND sc.name = ?
            """, table, schema)
            
            result = cursor.fetchone()
            if result and result[2]:
                return result[2]
            
            # Fallback: Generate DDL manually
            return self._generate_table_ddl(database, schema, table)
            
        except Exception as e:
            self.logger.warning(f"Could not extract DDL for {schema}.{table}: {e}")
            return f"-- Could not extract DDL for {schema}.{table}\n-- Error: {str(e)}"
    
    def _generate_table_ddl(self, database: str, schema: str, table: str) -> str:
        """Generate table DDL manually"""
        cursor = self.connection.cursor()
        cursor.execute(f"USE [{database}]")
        
        # Get column information
        cursor.execute(f"""
        SELECT 
            c.name,
            t.name + CASE 
                WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') 
                THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS varchar) END + ')'
                WHEN t.name IN ('decimal', 'numeric')
                THEN '(' + CAST(c.precision AS varchar) + ',' + CAST(c.scale AS varchar) + ')'
                ELSE ''
            END as data_type,
            c.is_nullable,
            c.is_identity,
            COLUMNPROPERTY(c.object_id, c.name, 'IsComputed') as is_computed
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(?, 'U')
        ORDER BY c.column_id
        """, f"{schema}.{table}")
        
        columns = cursor.fetchall()
        
        ddl = f"CREATE TABLE [{schema}].[{table}] (\n"
        
        for i, col in enumerate(columns):
            col_name, data_type, is_nullable, is_identity, is_computed = col
            nullable = "NULL" if is_nullable else "NOT NULL"
            identity = " IDENTITY(1,1)" if is_identity else ""
            
            ddl += f"    [{col_name}] {data_type}{identity} {nullable}"
            
            if i < len(columns) - 1:
                ddl += ","
            ddl += "\n"
        
        ddl += ");"
        
        return ddl
    
    def get_view_definition(self, database: str, schema: str, view: str) -> str:
        """Extract view definition"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
            SELECT definition
            FROM [{database}].sys.sql_modules m
            JOIN [{database}].sys.objects o ON m.object_id = o.object_id
            JOIN [{database}].sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type = 'V'
            AND o.name = ?
            AND s.name = ?
            """, view, schema)
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
        except Exception as e:
            self.logger.warning(f"Could not extract view definition for {schema}.{view}: {e}")
            
        return f"-- Could not extract view definition for {schema}.{view}"
    
    def get_stored_procedure_definition(self, database: str, schema: str, procedure: str) -> str:
        """Extract stored procedure definition"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
            SELECT definition
            FROM [{database}].sys.sql_modules m
            JOIN [{database}].sys.objects o ON m.object_id = o.object_id
            JOIN [{database}].sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type = 'P'
            AND o.name = ?
            AND s.name = ?
            """, procedure, schema)
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
        except Exception as e:
            self.logger.warning(f"Could not extract stored procedure definition for {schema}.{procedure}: {e}")
            
        return f"-- Could not extract stored procedure definition for {schema}.{procedure}"
    
    def create_folder_structure(self, server_name: str) -> Dict[ObjectType, Path]:
        """Create folder structure for server"""
        server_folder = self.output_dir / server_name.replace("\\", "_").replace("/", "_")
        server_folder.mkdir(parents=True, exist_ok=True)
        
        folders = {
            obj_type: server_folder / obj_type.value
            for obj_type in ObjectType
        }
        
        for folder in folders.values():
            folder.mkdir(exist_ok=True)
            
        return folders
    
    def save_object(self, folder: Path, database: str, obj_name: str, content: str, extension: str = ".sql"):
        """Save object definition to file"""
        # Sanitize object name for file system
        safe_obj_name = obj_name.replace("\\", "_").replace("/", "_").replace(":", "_")
        filename = f"{safe_obj_name}{extension}"
        filepath = folder / database / filename
        
        # Create database folder if it doesn't exist
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"-- Extracted on {datetime.now().isoformat()}\n")
            f.write(f"-- Database: {database}\n")
            f.write(f"-- Object: {obj_name}\n\n")
            f.write(content)
        
        self.logger.debug(f"Saved {obj_name} to {filepath}")
    
    def extract_all_objects(self):
        """Main extraction method"""
        if not self.connect():
            return False
        
        try:
            server_name = self.server
            folders = self.create_folder_structure(server_name)
            
            databases = self.get_databases()
            
            for database in databases:
                self.logger.info(f"Processing database: {database}")
                
                # Extract tables
                tables = self.get_tables(database)
                for table in tables:
                    ddl = self.get_table_ddl(database, table.schema, table.name)
                    self.save_object(folders[ObjectType.TABLE], database, table.full_name, ddl)
                
                # Extract views
                views = self.get_views(database)
                for view in views:
                    definition = self.get_view_definition(database, view.schema, view.name)
                    self.save_object(folders[ObjectType.VIEW], database, view.full_name, definition)
                
                # Extract stored procedures
                procedures = self.get_stored_procedures(database)
                for procedure in procedures:
                    definition = self.get_stored_procedure_definition(database, procedure.schema, procedure.name)
                    self.save_object(folders[ObjectType.PROCEDURE], database, procedure.full_name, definition)
                
                self.logger.info(f"Completed extraction for database: {database}")
            
            self.logger.info("Extraction completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during extraction: {e}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()
    
    def generate_report(self):
        """Generate extraction report"""
        report = {
            "extraction_date": datetime.now().isoformat(),
            "server": self.server,
            "output_directory": str(self.output_dir)
        }
        
        try:
            server_folder = self.output_dir / self.server.replace("\\", "_").replace("/", "_")
            if server_folder.exists():
                databases = {}
                
                for obj_type in ObjectType:
                    type_folder = server_folder / obj_type.value
                    if type_folder.exists():
                        for db_folder in type_folder.iterdir():
                            if db_folder.is_dir():
                                if db_folder.name not in databases:
                                    databases[db_folder.name] = {}
                                
                                files = list(db_folder.glob("*.sql"))
                                databases[db_folder.name][obj_type] = len(files)
                
                report["databases"] = databases
                
            report_file = self.output_dir / "extraction_report.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"Report generated: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating report: {e}")


def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract SQL Server objects")
    parser.add_argument("--server", help="SQL Server name")
    parser.add_argument("--username", help="SQL Server username")
    parser.add_argument("--password", help="SQL Server password")
    parser.add_argument("--port", type=int, default=1433, help="SQL Server port")
    parser.add_argument("--output", default="sql_extracted_objects", help="Output directory")
    parser.add_argument("--config", default="config.json", help="Configuration file")
    parser.add_argument("--no-trust-cert", action="store_true", help="Don't trust server certificate")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command line arguments
    server: str = args.server or config.get("server", "")
    username: str = args.username or config.get("username", "")
    password: str = args.password or config.get("password", "")
    port = args.port or config.get("port", 1433)
    output_dir = args.output or config.get("output_dir", "sql_extracted_objects")
    trust_cert = not args.no_trust_cert and config.get("trust_cert", True)
    
    if not all([server, username, password]):
        print("Error: Server, username, and password are required")
        print("Provide them via command line arguments or config.json file")
        return 1
    
    # Create extractor and run extraction
    extractor = SQLServerExtractor(
        server=server,
        username=username,
        password=password,
        output_dir=output_dir,
        port=port,
        trust_cert=trust_cert
    )
    
    success = extractor.extract_all_objects()
    extractor.generate_report()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())