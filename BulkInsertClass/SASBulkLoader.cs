using System.Data;
using System.Data.OleDb;
using Microsoft.Data.SqlClient;

namespace BulkInsertClass
{
    public class SASBulkLoader : BulkLoader, IBulkLoader
    {
        private string _oleDbConnectionString = string.Empty;
        private string _fileTableName = string.Empty;
        private string _inputFileSelectQuery = string.Empty;

        public SASBulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "")
            : base(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter)
        {
            SetTargetTable();
            SetOledbConnectionString();
        }

        public override void LoadToSql()
        {
            GetSasInputColumns();

            using (var targetConn = new SqlConnection(_sqlConnectionString))
            {
                targetConn.Open();
                targetConn.ChangeDatabase(_targetDatabase);

                _transferStart = DateTime.Now;
                _rowCountStart = (_overwrite == true) ? 0 : GetSqlRowCount(targetConn, _targetTable);

                _fileTableName = Path.GetFileName(InputFilePath);
                CreateDestinationTable(targetConn, _targetTable);

                LoadTable_SQLBulkCopy(targetConn);

                _transferFinish = DateTime.Now;
                _rowCountFinish = GetSqlRowCount(targetConn, _targetTable);
                LogImport(targetConn);

                Nullify(targetConn, _targetTable, _nullValue);
            }
        }

        private void GetSasInputColumns()
        {
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString))
            {
                oleDbConnection.Open();

                var schema = oleDbConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, null);
                schema.DefaultView.Sort = "ORDINAL_POSITION ASC";

                foreach (DataRow c in schema.Rows)
                {
                    var maxLength = new[] { Convert.ToInt32(c["COLUMN_SIZE"].ToString()), Convert.ToInt32(c["FORMAT_LENGTH"].ToString()) }.Max();
                    var isNullable = Convert.ToBoolean(c["IS_NULLABLE"].ToString());

                    TargetColumns.Add(new Column() { Name = c["COLUMN_NAME"].ToString(), DataType = "varchar", MaxLength = _defaultColumnWidth, IsNullable = isNullable });
                }

                if (ColumnsToKeep.Count > 0)
                    TargetColumns = TargetColumns.Where(t => ColumnsToKeep.Select(c => c.ToLower()).Contains(t.Name.ToLower())).ToList();

                _inputFileSelectQuery = _fileTableName;

                oleDbConnection.Close();
            }
        }

        private void SetOledbConnectionString()
        {
            var parentDirectory = Path.GetDirectoryName(InputFilePath);
            _oleDbConnectionString = "Provider=SAS Local Data Provider 9.44;Data Source='" + parentDirectory + "'";
        }

        /// <summary>
        /// IDataReader from SAS to SQL -- reliable for files with less than 255 columns.  Microsoft.Ace.OLEDB.12.0 cannot handle any more than that.
        /// </summary>
        /// <param name="targetConn"></param>
        private void LoadTable_SQLBulkCopy(SqlConnection targetConn)
        {
            Notify("Starting BulkCopy_Sas");
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString))
            {
                oleDbConnection.Open();
                using (var cmd = oleDbConnection.CreateCommand())
                {
                    cmd.CommandType = CommandType.TableDirect;
                    cmd.CommandText = _inputFileSelectQuery;
                    IDataReader data = cmd.ExecuteReader();

                    if (_headerRowsToSkip > 0)
                        Notify(string.Format("WARNING: HeaderRowsToSkip parameter has no effect when loading SAS files (HeaderRowsToSkip = {0}", _headerRowsToSkip));

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
