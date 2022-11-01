using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BulkInsertClass
{
    public class XLSBulkLoader : BulkLoader, IBulkLoader
    {
        private string _sheetName;
        private Dictionary<string, string> _targetTables = new Dictionary<string, string>();

        //Input file connection stuff
        private string _oleDbConnectionString;
        private string _fileTableName;
        private string _inputFileSelectQuery;

        public XLSBulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", string sheetName = "")
            : base(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter)
        {
            _sheetName = sheetName;
            SetTargetTables();
            SetOledbConnectionString();
        }

        public override void LoadToSql()
        {
            using (var targetConn = new SqlConnection(_sqlConnectionString))
            {
                targetConn.Open();
                targetConn.ChangeDatabase(_targetDatabase);

                _transferStart = DateTime.Now;
                _rowCountStart = (_overwrite == true) ? 0 : GetSqlRowCount(targetConn, _targetTable);

                GetXlsInputColumns();
                CreateDestinationTable(targetConn);
                LoadTable_SQLBulkCopy(targetConn);
                //ApplyDataTypes(targetConn, _targetTable);

                _transferFinish = DateTime.Now;
                _rowCountFinish = GetSqlRowCount(targetConn, _targetTable);
                // LogImport(targetConn);

                Nullify(targetConn, _targetTable, _nullValue);
            }
        }

        protected void SetTargetTables()
        {
            if (_sheetName != "")
            {
                // only load the worksheet with the name specified
                if (!_targetTable.Contains(".") && _targetSchema != "")
                {
                    _targetTable = _targetSchema + "." + _targetTable;
                }
                else {
                    _targetTable = GetWorksheetTableName(_sheetName);
                }
                _targetTables[_sheetName] = _targetTable;
            }
            else {
                // get all worksheet names and add them as separate target tables
                var worksheetNames = GetWorksheetNames();
                foreach (var worksheetName in worksheetNames) {
                    _targetTables[worksheetName] = GetWorksheetTableName(worksheetName);
                }
            }
        }

        private string GetWorksheetTableName(string worksheetName) {
            return _targetSchema + ".[" + worksheetName + "]";
        }

        public List<string> GetWorksheetNames()
        {
            List<string> sheets = new List<string>();
            using (OleDbConnection connection = new OleDbConnection(_oleDbConnectionString))
            {
                connection.Open();
                DataTable dt = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                foreach (DataRow drSheet in dt.Rows)
                    if (drSheet["TABLE_NAME"].ToString().Contains("$"))
                    {
                        string s = drSheet["TABLE_NAME"].ToString();
                        sheets.Add(s.StartsWith("'") ? s.Substring(1, s.Length - 3) : s.Substring(0, s.Length - 1));
                    }
                connection.Close();
            }
            return sheets;
        }

        private void SetOledbConnectionString()
        {
            var parentDirectory = Path.GetDirectoryName(InputFilePath);
            _oleDbConnectionString = "Provider=Microsoft.Ace.OLEDB.12.0;Data Source='" + InputFilePath + "';Extended Properties='Excel 12.0;IMEX=1;HDR=No';";
        }

        private void GetXlsInputColumns()
        {
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString.Replace("HDR=No", "HDR=Yes")))
            {
                oleDbConnection.Open();

                _inputFileSelectQuery = "SELECT ";
                var schema = oleDbConnection.GetSchema(SqlClientMetaDataCollectionNames.Columns);
                schema.DefaultView.Sort = "ORDINAL_POSITION ASC";

                string[] selectedColumns = new[] { "COLUMN_NAME", "ORDINAL_POSITION" };
                DataTable allColumnNames = new DataView(schema).ToTable(false, selectedColumns);
                DataTable distinctColumnNames = allColumnNames.DefaultView.ToTable( /*distinct*/ true);
                distinctColumnNames.DefaultView.Sort = "ORDINAL_POSITION";
                distinctColumnNames = distinctColumnNames.DefaultView.ToTable();

                foreach (DataRow c in distinctColumnNames.Rows)
                    TargetColumns.Add(new Column() { Name = c["COLUMN_NAME"].ToString(), DataType = "varchar", MaxLength = _defaultColumnWidth, IsNullable = true });

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

        private void LoadTable_SQLBulkCopy(SqlConnection targetConn)
        {
            Notify("Starting BulkCopy_Xls");
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString))
            {
                oleDbConnection.Open();

                _fileTableName = oleDbConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null).Rows[0]["TABLE_NAME"].ToString().Replace("'", "");
                GetXlsInputColumns();

                using (var cmd = new OleDbCommand(_inputFileSelectQuery, oleDbConnection))
                {
                    IDataReader data = cmd.ExecuteReader();

                    for (int i = 0; i < _headerRowsToSkip; i++)
                        data.Read();

                    if (_useHeaderRow)
                        data.Read();        //when we read from the source, we keep the colnames in the first row to force all columns to be varchar datatype

                    SqlBulkCopy bc = new SqlBulkCopy(targetConn);
                    bc.DestinationTableName = _targetTable;
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
