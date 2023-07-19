### We chose SQL alchemy over pyodbc because it has more design and higher level functions
from sqlalchemy import create_engine, text
from sqlalchemy.engine import result
import urllib
import pandas as pd

def connect_azure_database(database, sqlserver, username, password):
    """Connect to an Azure SQL database using pyodbc.

    Parameters:
        database (str): The name of the database to connect to.
        sqlserver (str): The name of the SQL Server hosting the database.
        username (str): The username to use when connecting to the database.
        password (str): The password to use when connecting to the database.

    Returns:
        Engine: A SQLAlchemy engine object representing the connection to the database.

    Raises:
        Exception: If the connection fails, an exception will be raised.
    """
    driver = "{ODBC Driver 17 for SQL Server}"
    serverfqdn = sqlserver + ".database.windows.net"
    connectionparams = urllib.parse.quote_plus(
        "Driver=%s;" % driver
        + "Server=tcp:%s,1433;" % serverfqdn
        + "Database=%s;" % database
        + "Uid=%s;" % username
        + "Pwd={%s};" % password
        + "Encrypt=yes;"
        + "TrustServerCertificate=no;"
        + "Connection Timeout=30;"
    )
    conn_str = "mssql+pyodbc:///?odbc_connect=" + connectionparams
    try:
        engine = create_engine(conn_str)
    finally:
        return engine


def sql_query_to_df(engine, sql_query, params=None):
    """
    Executes a SQL query on a database engine with optional parameters and returns the result as a pandas DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine representing the database connection.
        sql_query (str): The SQL query to execute.
        params (tuple or list): (optional) The parameters to be passed to the query.

    Returns:
        pandas.DataFrame: A DataFrame containing the result of the SQL query.

    Raises:
        Any error that occurs during the execution of the SQL query.

    Example:
        engine = create_engine('your_database_connection_string')
        query = 'SELECT * FROM your_table WHERE column1 = ? AND column2 = ?'
        params = ('value1', 'value2')
        result_df = sql_query_to_df(engine, query, params=params)
    """
    conn = engine.raw_connection()
    try:
        if params is not None:
            df = pd.read_sql_query(sql_query, conn, params=[params])
        else:
            df = pd.read_sql_query(sql_query, conn, params=params)
    finally:
        conn.close()
    return df

def process_logging_sp(
    engine,
    spName,
    ProcessStartTime,
    BusinessArea,
    ProcessName,
    ProcessStatus,
    SourceType,
    Source,
    SourceLocation,
    TargetType,
    Target,
    TargetLocation,
    ObjectType,
    ObjectName,
    RowsProcessed,
    ProcessMessage,
    RowsSkipped,
    ErrorMessage,
):
    """Execute a logging stored procedure in an Azure SQL database.

    Args:
        engine (Engine): A SQLAlchemy engine object representing the connection to the database.
        spName (str): The name of the stored procedure to execute.
        ProcessStartTime (str): The start time of the process being logged.
        BusinessArea (str): The business area the process belongs to.
        ProcessName (str): The name of the process being logged.
        ProcessStatus (str): The status of the process being logged.
        SourceType (str): The type of the source of the data being processed.
        Source (str): The source of the data being processed.
        SourceLocation (str): The location of the source of the data being processed.
        TargetType (str): The type of the target of the data being processed.
        Target (str): The target of the data being processed.
        TargetLocation (str): The location of the target of the data being processed.
        ObjectType (str): The type of the object being processed.
        ObjectName (str): The name of the object being processed.
        RowsProcessed (int): The number of rows processed.
        ProcessMessage (str): A message regarding the process being logged.
        RowsSkipped (int): The number of rows skipped during processing.
        ErrorMessage (str): An error message, if applicable.

    Returns:
        None
    """

    loggingSP = (
        "EXEC "
        + spName
        + " @ProcessStartTime=?, @BusinessArea=?, @ProcessName=?, @ProcessStatus=?, @SourceType=?, @Source=?, @SourceLocation=?, @TargetType=?, @Target=?, @TargetLocation=?, @ObjectType=?, @ObjectName=?, @RowsProcessed=?, @ProcessMessage=?, @RowsSkipped=?, @ErrorMessage=?"
    )
    loggingParams = (
        ProcessStartTime,
        BusinessArea,
        ProcessName,
        ProcessStatus,
        SourceType,
        Source,
        SourceLocation,
        TargetType,
        Target,
        TargetLocation,
        ObjectType,
        ObjectName,
        RowsProcessed,
        ProcessMessage,
        RowsSkipped,
        ErrorMessage,
    )
    connection = engine.raw_connection()
    try:
        cursor_obj = connection.cursor()
        cursor_obj.execute(loggingSP, loggingParams)
        cursor_obj.close()
        connection.commit()
    finally:
        connection.close()
