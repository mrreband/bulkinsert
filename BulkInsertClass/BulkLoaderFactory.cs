using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BulkInsertClass;

namespace BulkInsertClass
{
    public static class BulkLoaderFactory
    {
        public static List<string> GetSupportedExtensions()
        {
            return new List<string> { ".XLSX", ".XLS", ".CSV", ".TAB", ".SAS", ".SAS7BDAT", ".XML" };
        }

        public static IBulkLoader GetBulkLoader(string bulkLoaderType, string inputFilePath, string delimiter, string targetDatabase, string targetSchema, string targetTable, bool useHeaderRow, int headerRowsToSkip, bool overwrite, bool append, int batchSize, string sqlConnectionString, int DefaultColumnWidth = 1000, bool allowNulls = true, string nullValue = "", string comments = "", string schemaPath = "", string columnFilter = "", char quoteIdentifier = '"', char escapeCharacter = '"', string sheetName = "")
        {
            IBulkLoader bulkLoader = null;
            switch (bulkLoaderType.ToUpper())
            {
                case ".CSV":  case ".TAB":
                    bulkLoader = new CSVBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter, quoteIdentifier, escapeCharacter);
                    break;
                case ".XLSX": case ".XLS":
                    bulkLoader = new XLSBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter, sheetName);
                    break;
                case ".SAS":
                    bulkLoader = new SASBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter);
                    break;
                case ".XML":
                    bulkLoader = new XMLBulkLoader(inputFilePath, delimiter, targetDatabase, targetSchema, targetTable, useHeaderRow, headerRowsToSkip, overwrite, append, batchSize, sqlConnectionString, DefaultColumnWidth, allowNulls, nullValue, comments, schemaPath, columnFilter);
                    break;
                default:
                    throw new ArgumentException("Invalid bulkLoaderType");
            }
            
            return bulkLoader;
        }
    }
}
