using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BulkInsertClass;

namespace BulkInsert
{
    public static class BulkLoaderFactory
    {
        public static IBulkLoader GetBulkLoader(string bulkLoaderType, string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", char quoteIdentifier = '"', char escapeCharacter = '"')
        {
            IBulkLoader bulkLoader = null;
            switch (bulkLoaderType.ToUpper())
            {
                case "CSV":  case "TAB":
                    bulkLoader = new CSVBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, nullValue, comments, schemaPath, columnFilter, quoteIdentifier, escapeCharacter);
                    break;
                case "XLSX": case "XLS":
                    bulkLoader = new XLSBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, nullValue, comments, schemaPath, columnFilter);
                    break;
                case "SAS":
                    bulkLoader = new SASBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, nullValue, comments, schemaPath, columnFilter);
                    break;
                default:
                    throw new ArgumentException("Invalid Repository Type");
            }
            
            return bulkLoader;
        }
    }
}
