using CsvReader;
using System.Data;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace BulkInsertClass
{
    public class CSVBulkLoader : BulkLoader, IBulkLoader
    {
        //CSV-specific fields
        private string _schemaPath;
        private string _firstDataRow;
        private char _quoteIdentifier;
        private char _escapeCharacter;

        //Input file connection stuff
        private string _oleDbConnectionString;
        private string _fileTableName;
        private string _inputFileSelectQuery;

        public CSVBulkLoader(string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", char QuoteIdentifier = '"', char EscapeCharacter = '"')
            : base(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter)
        {
            _schemaPath = schemaPath;
            _quoteIdentifier = QuoteIdentifier;
            _escapeCharacter = EscapeCharacter;
            SetTargetTable();
            SetOledbConnectionString();
        }

        public override void LoadToSql()
        {
            using (var targetConn = new SqlConnection(_sqlConnectionString))
            {
                Console.WriteLine(_sqlConnectionString);
                Console.WriteLine(_targetDatabase);

                targetConn.Open();

                if (_targetDatabase != "")
                    targetConn.ChangeDatabase(_targetDatabase);

                _transferStart = DateTime.Now;
                _rowCountStart = (_overwrite == true) ? 0 : GetSqlRowCount(targetConn, _targetTable);

                _fileTableName = Path.GetFileName(InputFilePath);
                GetTextFileInputColumns();
                CreateDestinationTable(targetConn, _targetTable);

                if (TargetColumns.Count <= 255)
                    LoadTable_SQLBulkCopy_Csv(targetConn);
                else
                    LoadTable_SQLBulkCopy_LumenWorks(targetConn);

                _transferFinish = DateTime.Now;
                _rowCountFinish = GetSqlRowCount(targetConn, _targetTable);

                // ApplyDataTypes(targetConn, _targetTable);
                Nullify(targetConn, _targetTable, _nullValue);
            }
        }

        private void SetOledbConnectionString()
        {
            var parentDirectory = Path.GetDirectoryName(InputFilePath);
            _oleDbConnectionString = "Provider=Microsoft.Ace.OLEDB.12.0;Data Source='" + parentDirectory + "';Extended Properties='text;HDR=Yes;FMT=Delimited;CharacterSet=65001';";
        }

        private async void GetTextFileInputColumns()
        {
            var schemaFilePath = Path.Combine(Path.GetDirectoryName(InputFilePath), "Schema.ini");

            if (File.Exists(_schemaPath))
            {
                //schema.ini was provided -- read in the contents of schema.ini to populate TargetColumns
                File.Copy(_schemaPath, schemaFilePath, true);
                ReadSchemaIni(schemaFilePath);
            }
            else
            {
                if (_schemaPath != "")
                    Notify(string.Format("Warning: user supplied a value for SchemaPath, but the file {0} doesn't exist", _schemaPath));

                //schema.ini was not provided - read in the header row from the csv to get column names 
                //or at least the number of columns provided (if UseHeaderRows = false)
                //then create a generic schema.ini file with default data types and column widths
                using (var inputFile = await GetStreamReaderAsync(InputFilePath))
                {
                    for (int i = 0; i < _headerRowsToSkip; i++)
                        inputFile.ReadLine();

                    var headerRow = inputFile.ReadLine();
                    headerRow = Regex.Replace(headerRow, "[,]+$", "");                //exclude empty columns at the end

                    _firstDataRow = inputFile.ReadLine();
                    inputFile.Close();
                    foreach (var headerColumn in headerRow.Split(Delimiter))
                    {
                        TargetColumns.Add(new Column() { Name = headerColumn.Replace("\"", ""), DataType = "varchar", MaxLength = _defaultColumnWidth, IsNullable = _allowNulls });
                    }
                }

                MakeSchemaIni(schemaFilePath);
            }

            if (ColumnsToKeep.Count > 0)
            {
                var targetColumnNames = TargetColumns.Select(t => t.Name.ToLower()).ToList<string>();
                var unmatchedColumns = ColumnsToKeep.Where(c => !targetColumnNames.Contains(c.ToLower()));
                if (unmatchedColumns.Any())
                {
                    throw new KeyNotFoundException("Column(s) specified in ColumnFilter does not exist in the data: " + string.Join(",", unmatchedColumns));
                }
                TargetColumns = TargetColumns.Where(t => ColumnsToKeep.Select(c => c.ToLower()).Contains(t.Name.ToLower())).ToList();
                _inputFileSelectQuery = "SELECT " + string.Join(",", TargetColumns.Select(t => t.Name)) + " FROM [" + _fileTableName + "]";
            }
            else
            {
                _inputFileSelectQuery = "SELECT * FROM [" + _fileTableName + "]";
            }
        }

        /// <summary>
        /// parse a user-supplied schema.ini to get column definitions
        /// </summary>
        /// <param name="schemaFilePath"></param>
        private void ReadSchemaIni(string schemaFilePath)
        {
            Notify(string.Format("Reading provided Schema.ini file {0}", schemaFilePath));

            using (var schemaFile = new StreamReader(schemaFilePath))
            {
                var inputFilePath = schemaFile.ReadLine();
                var inputFileFormat = schemaFile.ReadLine();
                var hasColNameHeader = schemaFile.ReadLine();

                while (!schemaFile.EndOfStream)
                {
                    var colInfo = schemaFile.ReadLine();
                    if (Regex.IsMatch(colInfo, @"Col\d+\=", RegexOptions.IgnoreCase))
                    {
                        //valid column definition format: 
                        //Col{0}={1} Text Width {2}
                        colInfo = Regex.Replace(colInfo, @"Col\d+\=", "", RegexOptions.IgnoreCase);
                        var colInfoArray = colInfo.Split();

                        if (colInfoArray.Length != 4)
                            throw new Exception(string.Format("expected 4 parameters in schema.ini column definition, found {0} ({1})", colInfoArray.Length, colInfo));

                        var colName = colInfo.Split()[0];
                        var dataType = "varchar";  //colInfo.Split()[1];
                        var width = colInfo.Split()[2];
                        if (width.ToLower() != "width")
                            throw new Exception(string.Format("expected 3rd parameter in schema.ini column definition to be \"Width\", found {0}", width));

                        var maxLength = colInfo.Split()[3];
                        TargetColumns.Add(new Column() { Name = colName, DataType = dataType, MaxLength = Convert.ToInt32(maxLength), IsNullable = true });
                    }
                }
            }
        }

        /// <summary>
        /// create a generic schema.ini file with default data type and width values - this file is expected when using Microsoft.Ace.OLEDB driver
        /// </summary>
        /// <param name="schemaFilePath"></param>
        private void MakeSchemaIni(string schemaFilePath)
        {
            var schemaFile = new StreamWriter(schemaFilePath, false);
            schemaFile.WriteLine("[" + Path.GetFileName(InputFilePath) + "]");
            if (Delimiter == '\t')
                schemaFile.WriteLine("Format=TABDelimited");
            else
                schemaFile.WriteLine("Format=CSVDelimited");

            schemaFile.WriteLine("ColNameHeader=True");

            var i = 0;
            foreach (var targetColumn in TargetColumns)
            {
                schemaFile.WriteLine(string.Format("Col{0}={1} Text Width {2}", i + 1, GetSqlName(targetColumn.Name), targetColumn.MaxLength));
                i++;
            }
            schemaFile.Close();
        }

        private async Task<StreamReader> GetStreamReaderAsync(string filePath)
        {
            try
            {
                return new StreamReader(filePath);
            }
            catch (IOException)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));
                Notify("File is locked, waiting 5 seconds...");
                return await GetStreamReaderAsync(filePath);
            }
        }

        /// <summary>
        /// Extended IDataReader implementation, uses Lumenworks CsvReader class from nuget.  Can be more error-prone, but can also handle more than 255 columns. 
        /// </summary>
        /// <param name="targetConn"></param>
        private void LoadTable_SQLBulkCopy_LumenWorks(SqlConnection targetConn)
        {
            Notify("Starting BulkCopy_Csv_Lumenworks");
            using (var csvReader = new CsvReader.CsvReader(new StreamReader(InputFilePath), true, Delimiter, _quoteIdentifier, _escapeCharacter, '#', ValueTrimmingOptions.All))
            {
                csvReader.MissingFieldAction = MissingFieldAction.ReplaceByNull;

                for (int i = 0; i < _headerRowsToSkip; i++)
                    csvReader.ReadNextRecord();

                SqlBulkCopy bc = new SqlBulkCopy(targetConn)
                {
                    DestinationTableName = _targetTable,
                    BatchSize = _batchSize,
                    BulkCopyTimeout = 1800,
                    NotifyAfter = _batchSize
                };
                bc.SqlRowsCopied += new SqlRowsCopiedEventHandler(OnSqlRowsCopied);
                bc.WriteToServer(csvReader);
            }
        }

        /// <summary>
        /// Standard IDataReader to SQL -- reliable for files with less than 255 columns.  Microsoft.Ace.OLEDB.12.0 cannot handle any more than that.
        /// </summary>
        /// <param name="targetConn"></param>
        private void LoadTable_SQLBulkCopy_Csv(SqlConnection targetConn)
        {
            Notify("Starting BulkCopy_Csv");
            using (var oleDbConnection = new OleDbConnection(_oleDbConnectionString))
            {
                oleDbConnection.Open();
                using (var cmd = new OleDbCommand(_inputFileSelectQuery, oleDbConnection))
                {
                    IDataReader data = cmd.ExecuteReader();

                    for (int i = 0; i < _headerRowsToSkip; i++)
                        data.Read();

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
