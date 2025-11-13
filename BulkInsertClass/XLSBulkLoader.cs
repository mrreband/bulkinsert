using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.OleDb;
using System.Runtime.Versioning;

namespace BulkInsertClass
{
    [SupportedOSPlatform("windows")]
    public class XLSBulkLoader : BulkLoader, IBulkLoader
    {
        private string _sheetName;
        private Dictionary<string, string> _targetTables = new Dictionary<string, string>();

        //Input file connection stuff
        private string _oleDbConnectionString = string.Empty;
        private string _fileTableName = string.Empty;
        private string _inputFileSelectQuery = string.Empty;

        public XLSBulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", string sheetName = "")
            : base(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter)
        {
            // _targetTable was set in the base class but we don't want that
            _targetTable = targetTable;
            _sheetName = sheetName;

            SetOledbConnectionString();
            SetTargetTables();
        }

        public override void LoadToSql()
        {
            if (!OperatingSystem.IsWindows())
                throw new PlatformNotSupportedException("XLSBulkLoader requires Windows (OLE DB).");

            using (var targetConn = new SqlConnection(_sqlConnectionString))
            {
                targetConn.Open();
                targetConn.ChangeDatabase(_targetDatabase);

                foreach (string sheetName in _targetTables.Keys)
                {
                    string targetTable = _targetTables[sheetName];

                    _transferStart = DateTime.Now;
                    _rowCountStart = (_overwrite == true) ? 0 : GetSqlRowCount(targetConn, targetTable);

                    GetXlsInputColumns(sheetName);
                    CreateDestinationTable(targetConn, targetTable);
                    LoadTable_SQLBulkCopy(targetConn, sheetName, targetTable);
                    //ApplyDataTypes(targetConn, _targetTable);

                    _transferFinish = DateTime.Now;
                    _rowCountFinish = GetSqlRowCount(targetConn, targetTable);
                    // LogImport(targetConn);

                    Nullify(targetConn, targetTable, _nullValue);
                }
            }
        }

        protected void SetTargetTables()
        {
            if (_sheetName != "")
            {
                // only load the worksheet with the name specified
                if (_targetTable != "" && !_targetTable.Contains(".") && _targetSchema != "")
                {
                    _targetTable = _targetSchema + "." + _targetTable;
                }
                else
                {
                    _targetTable = GetWorksheetTableName(_sheetName);
                }
                _targetTables[_sheetName] = _targetTable;
            }
            else
            {
                // get all worksheet names and add them as separate target tables
                var worksheetNames = GetWorksheetNames();
                foreach (var worksheetName in worksheetNames)
                {
                    _targetTables[worksheetName] = GetWorksheetTableName(worksheetName);
                }
            }
        }

        private string GetWorksheetTableName(string worksheetName)
        {
            return _targetSchema + ".[" + worksheetName.Replace(" ", "_") + "]";
        }

        [SupportedOSPlatform("windows")]
        public List<string> GetWorksheetNames()
        {
            List<string> sheets = new List<string>();
            using (OleDbConnection connection = new(_oleDbConnectionString))
            {
                connection.Open();

                var rawTableNames = GetOleDbTableNames(connection);
                foreach (var raw in rawTableNames)
                {
                    if (string.IsNullOrWhiteSpace(raw))
                        continue;

                    int dollarIndex = raw.IndexOf('$');
                    if (dollarIndex >= 0)
                    {
                        var sheetName = dollarIndex == 0 ? raw : raw.Substring(0, dollarIndex);
                        sheets.Add(sheetName);
                    }
                }

                connection.Close();
            }
            return sheets;
        }

        private List<string> GetOleDbTableNames(OleDbConnection connection)
        {
            DataTable? dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
            if (dt == null)
                throw new InvalidOperationException($"No schema information for Excel file: {InputFilePath}");

            var list = new List<string>();
            using (dt)
            {
                foreach (DataRow dr in dt.Rows)
                {
                    object? tableObj = dr["TABLE_NAME"];
                    if (tableObj == null || tableObj == DBNull.Value)
                        continue;

                    string s = tableObj.ToString() ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(s))
                        continue;

                    list.Add(s.Replace("'", ""));
                }
            }

            return list;
        }

        private void SetOledbConnectionString()
        {
            var parentDirectory = Path.GetDirectoryName(InputFilePath);
            _oleDbConnectionString = "Provider=Microsoft.Ace.OLEDB.12.0;Data Source='" + InputFilePath + "';Extended Properties='Excel 12.0;IMEX=1;HDR=No';";
        }

        private void GetXlsInputColumns(string sheetName)
        {
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString.Replace("HDR=No", "HDR=Yes")))
            {
                oleDbConnection.Open();
                TargetColumns.Clear();

                _inputFileSelectQuery = "SELECT ";

                if (string.IsNullOrEmpty(_fileTableName))
                {
                    var rawNames = GetOleDbTableNames(oleDbConnection);
                    if (rawNames.Count == 0)
                        throw new InvalidOperationException($"Unable to determine table name from Excel file: {InputFilePath}");
                    _fileTableName = rawNames.First();
                }

                // tableRestrictions = { TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME }
                string[] tableRestrictions = new string[4];
                tableRestrictions[2] = _fileTableName;

                DataTable? schema = oleDbConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, tableRestrictions);
                if (schema == null)
                    throw new InvalidOperationException($"Unable to retrieve column schema for sheet '{sheetName}' in file: {InputFilePath}");

                schema.DefaultView.Sort = "ORDINAL_POSITION ASC";

                string[] selectedColumns = new[] { "COLUMN_NAME", "ORDINAL_POSITION" };

                DataTable allColumns = new DataView(schema).ToTable(false);
                DataTable allColumnNames = allColumns.DefaultView.ToTable(false, selectedColumns);
                DataTable distinctColumnNames = allColumnNames.DefaultView.ToTable(true);
                distinctColumnNames.DefaultView.Sort = "ORDINAL_POSITION";
                distinctColumnNames = distinctColumnNames.DefaultView.ToTable();

                foreach (DataRow c in distinctColumnNames.Rows)
                {
                    object? colObj = c["COLUMN_NAME"];
                    if (colObj == null || colObj == DBNull.Value)
                        continue;

                    string columnName = colObj.ToString() ?? string.Empty;
                    if (string.IsNullOrEmpty(columnName))
                        continue;

                    TargetColumns.Add(new Column() { Name = columnName, DataType = "varchar", MaxLength = _defaultColumnWidth, IsNullable = true });
                }

                if (ColumnsToKeep.Count > 0)
                {
                    TargetColumns = TargetColumns.Where(t => ColumnsToKeep.Select(c => c.ToLower()).Contains(t.Name.ToLower())).ToList();
                    _inputFileSelectQuery = "SELECT " + string.Join(",", TargetColumns.Select(t => "[" + t.Name + "]")) + " FROM [" + _fileTableName + "A1:ZZ]";
                }
                else
                {
                    _inputFileSelectQuery = "SELECT * FROM [" + _fileTableName + "A1:ZZ]";
                }

                oleDbConnection.Close();
            }
        }

        private void LoadTable_SQLBulkCopy(SqlConnection targetConn, string sheetName, string targetTable)
        {
            Notify(String.Format("Starting BulkCopy_Xls - {0} --> {1}", sheetName, targetTable));
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString))
            {
                oleDbConnection.Open();

                var tableNames = GetOleDbTableNames(oleDbConnection);
                if (tableNames.Count == 0)
                    throw new InvalidOperationException($"Unable to determine table name from Excel file: {InputFilePath}");
                _fileTableName = tableNames[0];

                GetXlsInputColumns(sheetName);

                using (var cmd = new OleDbCommand(_inputFileSelectQuery, oleDbConnection))
                {
                    IDataReader data = cmd.ExecuteReader();

                    for (int i = 0; i < _headerRowsToSkip; i++)
                        data.Read();

                    if (_useHeaderRow)
                        data.Read();        //when we read from the source, we keep the colnames in the first row to force all columns to be varchar datatype

                    SqlBulkCopy bc = new SqlBulkCopy(targetConn);
                    bc.DestinationTableName = targetTable;
                    bc.BatchSize = _batchSize;
                    bc.BulkCopyTimeout = 600;
                    bc.NotifyAfter = _batchSize;
                    bc.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
                    bc.WriteToServer(data);
                }
            }
        }
    }
}
