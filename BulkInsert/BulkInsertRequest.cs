using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsert
{
    public class BulkInsertRequest
    {
        private string InputFilePath { get; set; }
        private string FileExtensionOverride { get; set; }
        private string Delimiter { get; set; }
        private string TargetServer { get; set; }
        private string TargetDatabase { get; set; }
        private string TargetSchema { get; set; }
        private string TargetTable { get; set; }
        private bool UseHeader { get; set; }
        private int HeaderRowsToSkip { get; set; }
        private bool CopyLocal { get; set; }
        private bool Overwrite { get; set; }
        private bool Append { get; set; }
        private int DefaultColumnWidth { get; set; }
        private int BatchSize { get; set; }
        private string Comments { get; set; }
        private string SchemaPath { get; set; }
        private string ColumnFilter { get; set; }
        private char QuoteIdentifier { get; set; }
        private char EscapeCharacter { get; set; }
        private bool AllowNulls { get; set; }
        private string NullValue { get; set; }
        private string TargetConnectionString { get; set; }
        
        private string SheetName { get; set;  }

        public BulkInsertRequest(string inputFilePath, string delimiter, string targetServer, string targetDatabase, string targetSchema, string targetTable, bool useHeader, int headerRowsToSkip, bool copyLocal, bool overwrite, bool append, int defaultColumnWidth,
            int batchSize, string comments, string schemaPath, string columnFilter, char quoteIdentifier, char escapeCharacter, bool allowNulls, string nullValue)
        {
            this.InputFilePath = inputFilePath;
            this.Delimiter = delimiter;
            this.TargetServer = targetServer;
            this.TargetDatabase = targetDatabase;
            this.TargetSchema = targetSchema;
            this.TargetTable = targetTable;
            this.UseHeader = useHeader;
            this.HeaderRowsToSkip = headerRowsToSkip;
            this.CopyLocal = copyLocal;
            this.Overwrite = overwrite;
            this.Append = append;
            this.DefaultColumnWidth = defaultColumnWidth;
            this.BatchSize = batchSize;
            this.Comments = comments;
            this.SchemaPath = schemaPath;
            this.ColumnFilter = ColumnFilter;
            this.QuoteIdentifier = quoteIdentifier;
            this.EscapeCharacter = escapeCharacter;
            this.AllowNulls = allowNulls;
            this.NullValue = NullValue;
        }

        public BulkInsertRequest(Dictionary<string, string> bulkLoadParameters, string targetConnectionString)
        {
            this.InputFilePath = bulkLoadParameters["InputFilePath"];
            this.FileExtensionOverride = bulkLoadParameters["FileExtensionOverride"].ToLower();
            this.Delimiter = bulkLoadParameters["Delimiter"];
            this.TargetServer = bulkLoadParameters["TargetServer"];
            this.TargetDatabase = bulkLoadParameters["TargetDatabase"];
            this.TargetSchema = bulkLoadParameters["TargetSchema"];
            this.TargetTable = bulkLoadParameters["TargetTable"];
            this.UseHeader = Convert.ToBoolean(bulkLoadParameters["UseHeader"]);
            this.HeaderRowsToSkip = Convert.ToInt32(bulkLoadParameters["HeaderRowsToSkip"]);
            this.CopyLocal = Convert.ToBoolean(bulkLoadParameters["CopyLocal"]);
            this.Overwrite = Convert.ToBoolean(bulkLoadParameters["Overwrite"]);
            this.Append = Convert.ToBoolean(bulkLoadParameters["Append"]);
            this.DefaultColumnWidth = Convert.ToInt32(bulkLoadParameters["DefaultColumnWidth"]);
            this.BatchSize = Convert.ToInt32(bulkLoadParameters["BatchSize"]);
            this.Comments = bulkLoadParameters["Comments"];
            this.SchemaPath = bulkLoadParameters["SchemaPath"];
            this.ColumnFilter = bulkLoadParameters["ColumnFilter"];
            this.QuoteIdentifier = bulkLoadParameters["QuoteIdentifier"].ToCharArray()[0];
            this.EscapeCharacter = bulkLoadParameters["EscapeCharacter"].ToCharArray()[0];
            this.AllowNulls = Convert.ToBoolean(bulkLoadParameters["AllowNulls"]);
            this.NullValue = bulkLoadParameters["NullValue"];
            this.SheetName = bulkLoadParameters["SheetName"];

            this.TargetConnectionString = targetConnectionString;
            SetTargetConnectionString(TargetConnectionString);
        }

        public bool ProcessRequest()
        {
            if (!File.Exists(InputFilePath))
                throw new FileNotFoundException(string.Format("Input File {0} was not found", InputFilePath));

            var fileExtension = (this.FileExtensionOverride != "")
                ? this.FileExtensionOverride
                : Path.GetExtension(InputFilePath).ToLower().Replace(".", "");

            var allowedExtensions = new List<String>() { "csv", "xlsx", "sas7bdat", "tab", "xml" };
            if (fileExtension == "zip")
            {
                InputFilePath = UnzipFile(InputFilePath);
                fileExtension = Path.GetExtension(InputFilePath).ToLower().Replace(".", "");
            }
            else if (!allowedExtensions.Contains(fileExtension))
            throw new NotImplementedException("Only csv, xlsx, xml, sas7bdat files are supported"); 

            var localFolderPath = System.IO.Path.GetTempPath();

            var localFilePath = InputFilePath;
            if (CopyLocal)
            {
                if (!Directory.Exists(localFolderPath)) Directory.CreateDirectory(localFolderPath);
                var inputFileName = GetValidAceDbFileName(Path.GetFileName(InputFilePath));
                localFilePath = Path.Combine(localFolderPath, inputFileName);
                CopyFileForReadWrite(InputFilePath, localFilePath);
            }

            var bl = BulkLoaderFactory.GetBulkLoader(fileExtension, localFilePath, Delimiter, TargetDatabase, TargetSchema, TargetTable, Convert.ToBoolean(UseHeader), Convert.ToInt32(HeaderRowsToSkip), Convert.ToBoolean(Overwrite), Convert.ToBoolean(Append), Convert.ToInt32(BatchSize), TargetConnectionString, Convert.ToInt32(DefaultColumnWidth), Convert.ToBoolean(AllowNulls), NullValue, Comments, SchemaPath, ColumnFilter, QuoteIdentifier, EscapeCharacter, SheetName);

            bl.Notifier += Notify;
            bl.LoadToSql();

            var schemaFile = Path.Combine(localFolderPath, "Schema.ini");
            if (File.Exists(schemaFile)) File.Delete(schemaFile);

            if (CopyLocal)
            {
                Console.WriteLine("Deleting " + localFilePath);
                File.Delete(localFilePath);
                if (!Directory.EnumerateFileSystemEntries(localFolderPath).Any()) Directory.Delete(localFolderPath);
            }
            return true;
        }
        private static void CopyFileForReadWrite(string sourceFile, string targetFile)
        {
            Console.WriteLine(string.Format("Copying {0} to {1} for ReadWrite", sourceFile, targetFile));
            File.Copy(sourceFile, targetFile, true);
            var attributes = File.GetAttributes(targetFile);
            if ((attributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
            {
                attributes = RemoveAttribute(attributes, FileAttributes.ReadOnly);
                File.SetAttributes(targetFile, attributes);
            }
        }

        private static FileAttributes RemoveAttribute(FileAttributes attributes, FileAttributes attributesToRemove)
        {
            return attributes & ~attributesToRemove;
        }

        private void SetTargetConnectionString(String targetConnectionString)
        {
            if (TargetServer != "")
            {
                TargetConnectionString = System.Text.RegularExpressions.Regex.Replace(targetConnectionString, @"data source\=\w+", "Data Source=" + TargetServer, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            }
        }

        static string GetValidAceDbFileName(string fileName)
        {
            fileName = Path.GetFileNameWithoutExtension(fileName).Replace(".", "");
            if (fileName.Length <= 64) return fileName + ".csv";
            return fileName.Substring(0, 60) + ".csv";
        }

        static string UnzipFile(string zipFilePath)
        {
            var unzippedFilePath = "";
            using (var zippedFiles = ZipFile.OpenRead(zipFilePath))
            {
                if (zippedFiles.Entries.Count != 1)
                    throw new NotImplementedException("Expected only one file in " + zipFilePath);

                var unzippedFileName = Path.GetFileName(zippedFiles.Entries[0].FullName);
                unzippedFilePath = Path.Combine(Path.GetDirectoryName(zipFilePath), unzippedFileName);
                if (File.Exists(unzippedFilePath)) File.Delete(unzippedFilePath);

                Console.WriteLine("Unzipping " + unzippedFileName);
                ZipFile.ExtractToDirectory(zipFilePath, Path.GetDirectoryName(zipFilePath));
            }
            return unzippedFilePath;
        }

        static void Notify(object sender, BulkInsertClass.NotifyEventArgs args)
        {
            Console.WriteLine(string.Format("{0}: {1}", args.TargetTable, args.Message));
        }
    }
}
