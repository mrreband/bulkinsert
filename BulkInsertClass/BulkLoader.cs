using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

using System.Linq;

namespace BulkInsertClass
{
    public abstract class BulkLoader
    {
        public string InputFilePath { get; set; }
        public char Delimiter { get; set; }
        
        //user parameters passed in 
        protected bool _overwrite;
        protected bool _append;
        protected string _targetDatabase;
        protected string _targetTable;
        protected string _targetSchema;
        protected int _batchSize;
        protected int _headerRowsToSkip;
        protected bool _useHeaderRow;

        //For building SQL Statements and binding columns
        protected List<Column> TargetColumns;
        protected List<string> ColumnsToKeep;

        //target sql connection
        protected string _sqlConnectionString;
        protected int _defaultColumnWidth;
        protected string _nullValue;

        //stuff for logging
        protected int _rowCountStart = 0;
        protected int _rowCountFinish = 0;
        protected DateTime _transferStart;
        protected DateTime _transferFinish;
        protected string _comments;

        public BulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "")
        {
            InputFilePath = inputFilePath;
            Delimiter = (delimiter == "\\t") ? '\t' : delimiter.ToCharArray()[0];
            _headerRowsToSkip = headerRowsToSkip;
            _useHeaderRow = useHeaderRow;

            _overwrite = overwrite;
            _append = append;
            _targetDatabase = targetDatabase;
            _targetSchema = targetSchema;
            _targetTable = targetTable;
            _batchSize = batchSize;
            _sqlConnectionString = sqlConnectionString;
            _defaultColumnWidth = DefaultColumnWidth;
            _comments = (comments == "") ? "Load from BulkLoader" : comments + " (from BulkLoader)";
            TargetColumns = new List<Column>();
            
            if (columnFilter != "")
                ColumnsToKeep = columnFilter.Split(',').ToList();
            else
                ColumnsToKeep = new List<string>();

            _nullValue = nullValue;
            SetTargetTable();
        }

        public virtual void LoadToSql()
        {
            //This is up to the concrete class
            throw new NotImplementedException();
        }

        protected void Nullify(SqlConnection targetConn, string tableName, string nullValue)
        {
            if (_nullValue != "''")
            {
                Notify(string.Format("Updating Null Values: {0} --> NULL", nullValue));
                var nullifyQuery = string.Format("exec sp_Nullify '{0}', '{1}'", tableName, nullValue);
                using (var cmd = new SqlCommand(nullifyQuery, targetConn))
                    cmd.ExecuteNonQuery();
            }
        }

        protected int GetSqlRowCount(SqlConnection targetConn, string targetTable)
        {
            using (var rcCmd = new SqlCommand(string.Format(@"  IF OBJECT_ID('{0}') IS NOT NULL BEGIN 
                                                                    DROP TABLE IF EXISTS #spaceused
                                                                    CREATE TABLE #spaceused
                                                                    (
	                                                                    name VARCHAR(256), 
	                                                                    rows BIGINT, 
	                                                                    reserved VARCHAR(256), 
	                                                                    data VARCHAR(256), 
	                                                                    index_size VARCHAR(256), 
	                                                                    unused VARCHAR(256)
                                                                    )
                                                                    INSERT INTO #spaceused
                                                                    EXEC sp_spaceused '{0}'

                                                                    SELECT rows FROM #spaceused
                                                                END
                                                                ELSE SELECT 0 as rows", targetTable), targetConn))
                return Convert.ToInt32(rcCmd.ExecuteScalar().ToString());
        }

        protected void ApplyDataTypes(SqlConnection targetConn, string targetTable)
        {
            using (var rcCmd = new SqlCommand(string.Format(@"  exec sp_suggestdatatypes '{0}';
                                                                exec sp_applydatatypesuggestions '{0}'", targetTable), targetConn))
                rcCmd.ExecuteNonQuery();
        }

        protected void CreateDestinationTable(SqlConnection targetConnection)
        {
            var tableExists = false;
            using (var tableExistsCmd = new SqlCommand("SELECT ISNULL(OBJECT_ID(@targetTable,'U'), -1)", targetConnection))
            {
                tableExistsCmd.CommandType = CommandType.Text;
                tableExistsCmd.Parameters.AddWithValue("@targetTable", _targetTable);
                tableExists = ((int)tableExistsCmd.ExecuteScalar() == -1) ? false : true;
            }

            ///////////////////////////////////////////////////////////////////////////

            if (!tableExists || _overwrite)
            {
                //drop existing table if applicable
                if (tableExists && _overwrite)  
                {
                    Notify(string.Format("Dropping existing table {0}", _targetTable));
                    var dropTableSyntax = string.Format("DROP TABLE {0}", _targetTable);
                    using (var dropTableCmd = new SqlCommand(dropTableSyntax, targetConnection))
                    {
                        dropTableCmd.ExecuteNonQuery();
                    }
                }

                //create table sql syntax
                Notify(string.Format("Creating Target Table {0}; Overwrite = {1}; Append = {2}", _targetTable, _overwrite, _append));
                var createTableSql = string.Format("CREATE TABLE {0} (", _targetTable);
                int i = 1;
                foreach (var column in TargetColumns)
                {
                    var length = (column.MaxLength == -1) ? "MAX" : column.MaxLength.ToString();
                    createTableSql += string.Format("{0} varchar({1}) {2},", GetSqlName(column.Name), length, (column.IsNullable ? "NULL" : "NOT NULL"));
                    i++;
                    if (i >= 1024)
                    {
                        Notify("1024 Column Limit Reached");
                        break;
                    }
                }
                createTableSql = createTableSql.TrimEnd(',') + ");";

                //execute sql
                using (var createTableCmd = new SqlCommand(createTableSql, targetConnection))
                    createTableCmd.ExecuteNonQuery();
            }

            if (tableExists && !_overwrite && !_append)
                throw new Exception(string.Format("Table {0} Already Exists -- use overwrite flag to overwrite or append flag to append", _targetTable));

            ////for completeness: 
            ////if (tableExists && !overwrite && append)
            ////    do nothing -- user wants to append to an existing table
        }

        protected void LogImport(SqlConnection targetConn)
        {
            Notify(string.Format("StartTime = {0}; Finishtime = {1}", _transferStart, _transferFinish));

            using (var logImportCmd = new SqlCommand("RawData.ETL.LogImport", targetConn))
            {
                logImportCmd.CommandType = CommandType.StoredProcedure;
                logImportCmd.Parameters.AddWithValue("@InputFilePath", InputFilePath);
                logImportCmd.Parameters.AddWithValue("@TargetTable", _targetDatabase + '.' + _targetTable);
                logImportCmd.Parameters.AddWithValue("@StartTime", _transferStart);
                logImportCmd.Parameters.AddWithValue("@FinishTime", _transferFinish);
                logImportCmd.Parameters.AddWithValue("@Comments", _comments);
                logImportCmd.CommandTimeout = 60;
                logImportCmd.ExecuteNonQuery();
            }

            var rowsLoaded = _rowCountFinish - _rowCountStart;
            var elapsedTime = (_transferFinish - _transferStart).TotalSeconds;

            Notify("Finished Loading");
            Notify(string.Format("Total time:  {0} seconds", elapsedTime));
            Notify(string.Format("Rows loaded:  {0}", rowsLoaded));
            if (elapsedTime == 0) elapsedTime = 1;
            Notify(string.Format("Rows per second:  {0}", rowsLoaded / elapsedTime));
        }

        protected void SetTargetTable()
        {
            if (_targetTable == "")
            {
                _targetTable = _targetSchema + ".[" + Regex.Replace(Path.GetFileNameWithoutExtension(InputFilePath), @"[^\w]", "_") + "]";
                Notify(string.Format("TargetTable Not Specified -- using auto-generated table name {0}", _targetTable));
            }
            else if (!_targetTable.Contains(".") && _targetSchema != "")
            {
                _targetTable = _targetSchema + "." + _targetTable;
            }
        }

        protected string GetSqlName(string rawName)
        {
            var sqlName = Regex.Replace(rawName, @"[^\w]+", "_").Trim('_');
            sqlName = "[" + Regex.Replace(sqlName, @"^[\d]+", "") + "]";
            return sqlName;
        }

        protected void OnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Notify(string.Format("{0} rows loaded", e.RowsCopied));
        }

        ////////////////////////////////////////////////////////////////////////
        public event EventHandler<NotifyEventArgs> Notifier;
        protected virtual void OnNotify(NotifyEventArgs e)
        {
            Notifier?.Invoke(this, e);
        }

        protected void Notify(string message)
        {
            OnNotify(new NotifyEventArgs() { Message = message, InputFilePath = InputFilePath, TargetTable = _targetTable });
        }
    }

    public class Column
    {
        public string Name { get; set; }
        public string DataType { get; set; }

        public int MaxLength { get; set; }
        public bool IsNullable { get; set; }
    }
}
