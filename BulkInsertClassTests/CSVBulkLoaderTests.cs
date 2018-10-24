using Microsoft.VisualStudio.TestTools.UnitTesting;
using BulkInsertClass;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BulkInsert;

namespace BulkInsertClass.Tests
{
    [TestClass()]
    public class CSVBulkLoaderTests
    {
        string CsvInputFilePath = @"C:\Data\Incoming\test.csv";
        string XlsxInputFilePath = @"C:\Data\Incoming\test.xlsx";
        string UseInputQueue = "false";
        string Delimiter = ",";
        string TargetServer = "mpaserver35";
        string TargetDatabase = "RawData";
        string TargetSchema = "dbo";
        string TargetTable = "";
        bool UseHeader = true;
        int HeaderRowsToSkip = 0;
        bool CopyLocal = true;
        bool Overwrite = true;
        bool Append = false;
        int DefaultColumnWidth = 1000;  
        int BatchSize = 10000;
        string Comments = "testing 123";
        string SchemaPath = "";
        int MaxDOP = 2;
        string ColumnFilter = "";
        string NullValue = "''";
        string targetConnectionString = "Data Source=mpaserver35;Initial Catalog=RawData;Integrated Security=SSPI";

        [TestMethod()]
        public void CSVBulkLoaderTest()
        {
            var bl = BulkLoaderFactory.GetBulkLoader("CSV", CsvInputFilePath, Delimiter, TargetDatabase, TargetSchema, TargetTable, UseHeader, HeaderRowsToSkip, Overwrite, Append, BatchSize, targetConnectionString);
            bl.LoadToSql();            
        }

        [TestMethod()]
        public void XLSBulkLoaderTest()
        {
            //note: this relies on Microsoft.ACE.OLEDB provider, which is x64 only, so make sure your test processor architecture = x64
            var bl = BulkLoaderFactory.GetBulkLoader("XLS", XlsxInputFilePath, Delimiter, TargetDatabase, TargetSchema, TargetTable, UseHeader, HeaderRowsToSkip, Overwrite, Append, BatchSize, targetConnectionString);
            bl.LoadToSql();
        }
    }
}