using System.Data;
using Microsoft.Data.SqlClient;


namespace BulkInsertClass
{
    public class XMLBulkLoader : BulkLoader, IBulkLoader
    {
        //xml-specific 
        private DataTable _dt;
        private string _inputFilePath = string.Empty;

        //Input file connection stuff
        private string _oleDbConnectionString = string.Empty;
        private string _fileTableName = string.Empty;

        public XMLBulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", char QuoteIdentifier = '"', char EscapeCharacter = '"')
            : base(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter)
        {
            _inputFilePath = inputFilePath;
            _dt = GetSourceTable();
            SetTargetTable();
            SetOledbConnectionString();
        }

        public override void LoadToSql()
        {
            using (var targetConn = new SqlConnection(_sqlConnectionString))
            {
                targetConn.Open();
                if (_targetDatabase != "")
                    targetConn.ChangeDatabase(_targetDatabase);

                _transferStart = DateTime.Now;
                _rowCountStart = (_overwrite == true) ? 0 : GetSqlRowCount(targetConn, _targetTable);

                _fileTableName = Path.GetFileName(InputFilePath);
                GetXMLColumns();
                CreateDestinationTable(targetConn, _targetTable);
                LoadTable_SQLBulkCopy_Xml(targetConn);

                _transferFinish = DateTime.Now;
                _rowCountFinish = GetSqlRowCount(targetConn, _targetTable);
                //LogImport(targetConn);

                Nullify(targetConn, _targetTable, _nullValue);
            }
        }

        private void SetOledbConnectionString()
        {
            var parentDirectory = Path.GetDirectoryName(InputFilePath);
            _oleDbConnectionString = "Provider=Microsoft.Ace.OLEDB.12.0;Data Source='" + parentDirectory + "';Extended Properties='text;HDR=Yes;FMT=Delimited';";
        }

        private DataTable GetSourceTable()
        {
            var ds = new DataSet();
            ds.ReadXml(_inputFilePath);
            return ds.Tables[0];
        }

        private void GetXMLColumns()
        {
            foreach (DataColumn col in _dt.Columns)
            {
                TargetColumns.Add(new Column() { Name = col.ColumnName, DataType = "varchar", MaxLength = _defaultColumnWidth, IsNullable = true });
            }
        }

        /// <summary>
        /// Standard IDataReader to SQL -- reliable for files with less than 255 columns.  Microsoft.Ace.OLEDB.12.0 cannot handle any more than that.
        /// </summary>
        /// <param name="targetConn"></param>
        private void LoadTable_SQLBulkCopy_Xml(SqlConnection targetConn)
        {
            Notify("Starting BulkCopy_Xml");

            SqlBulkCopy bc = new SqlBulkCopy(targetConn);
            bc.DestinationTableName = _targetTable;
            bc.BatchSize = _batchSize;
            bc.BulkCopyTimeout = 600;
            bc.NotifyAfter = _batchSize;
            bc.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
            bc.WriteToServer(_dt);
        }
    }
}
