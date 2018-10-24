using System;
using System.Net;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO.Compression;

namespace BulkInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            var bulkLoadParameters = GetDefaultParameters();            
            var copyLocal = Convert.ToBoolean(bulkLoadParameters["CopyLocal"]);

            //cap max degrees of parallelism between 1 and 8
            var maxDOP = Convert.ToInt32(ConfigurationManager.AppSettings["MaxDOP"].ToString());
            maxDOP = (maxDOP < 1) ? 1 : (maxDOP > 8) ? 8 : maxDOP;

            foreach (string s in args)
            {
                var keyValuePair = s.Trim().Split('=');
                if (keyValuePair.Count() != 2) throw new ArgumentException("Improper Argument Format (should be key=value): " + s);
                if (!bulkLoadParameters.ContainsKey(keyValuePair[0])) throw new ArgumentException("Argument not supported: " + s);
                Console.WriteLine(s);
                bulkLoadParameters[keyValuePair[0]] = keyValuePair[1];
            }

            if (bulkLoadParameters["UseInputQueue"].ToLower() == "true")
            {
                if (File.Exists(bulkLoadParameters["InputFilePath"]))
                {
                    ProcessInputQueueFile(bulkLoadParameters["InputFilePath"], maxDOP, copyLocal);
                    return;
                }
                else
                    throw new FileNotFoundException(string.Format("UseInputQueue parameter specified, but file {0} was not found", bulkLoadParameters["InputFilePath"]));
            }

            //if a directory is specified, process all files in the directory
            FileAttributes attr = File.GetAttributes(bulkLoadParameters["InputFilePath"]);
            if ((attr & FileAttributes.Directory) == FileAttributes.Directory)
                ProcessFilesInDirectory(bulkLoadParameters, maxDOP, copyLocal);
            else
                ProcessFile(bulkLoadParameters, copyLocal);

            Console.WriteLine("done");
        }

        static void ProcessFiles(List<Dictionary<string, string>> bulkLoadQueue, bool copyLocal, int maxDOP)
        {
            Parallel.ForEach(
                bulkLoadQueue,
                new ParallelOptions { MaxDegreeOfParallelism = maxDOP },
                bulkLoadParameters => {
                    var localFilePath = Path.GetFileName(bulkLoadParameters["InputFilePath"]);
                    CopyFileForReadWrite(bulkLoadParameters["InputFilePath"], localFilePath);

                    try
                    {
                        ProcessFile(bulkLoadParameters, copyLocal);
                    }
                    catch (Exception ex)
                    {
                        var errLog = new StreamWriter("err.log", false);
                        errLog.WriteLine(bulkLoadParameters["InputFilePath"] + ": " + ex.Message + " at " + ex.StackTrace);
                        errLog.Close();
                    }

                    File.Delete(localFilePath);
                }
            );
        }

        private static void ProcessFile(Dictionary<string, string> bulkLoadParameters, bool copyLocal)
        {
            var inputFilePath = bulkLoadParameters["InputFilePath"];

            if (!File.Exists(inputFilePath))
                throw new FileNotFoundException(string.Format("Input File {0} was not found", inputFilePath));
            
            var fileExtension = Path.GetExtension(inputFilePath).ToLower().Replace(".", "");
            if (fileExtension == "zip")
            {
                inputFilePath = UnzipFile(inputFilePath);
                fileExtension = Path.GetExtension(inputFilePath).ToLower().Replace(".", "");
                bulkLoadParameters["InputFilePath"] = inputFilePath;
            }
            else if (!(fileExtension == "csv" || fileExtension == "xlsx" || fileExtension == "sas7bdat" || fileExtension == "tab"))
                throw new NotImplementedException("Only csv, xlsx, sas7bdat files are supported");

            var localFolderPath = GetValidAceDbFileName(Path.GetFileNameWithoutExtension(bulkLoadParameters["InputFilePath"]));

            var localFilePath = inputFilePath;
            if (copyLocal)
            {
                if (!Directory.Exists(localFolderPath)) Directory.CreateDirectory(localFolderPath);
                var inputFileName = GetValidAceDbFileName(Path.GetFileName(bulkLoadParameters["InputFilePath"]));
                localFilePath = Path.Combine(localFolderPath, inputFileName);
                CopyFileForReadWrite(bulkLoadParameters["InputFilePath"], localFilePath);                
            }

            var targetConnectionString = GetTargetConnectionString(bulkLoadParameters);

            var bl = BulkLoaderFactory.GetBulkLoader(fileExtension, 
                                                        localFilePath,
                                                        bulkLoadParameters["Delimiter"],
                                                        bulkLoadParameters["TargetDatabase"],
                                                        bulkLoadParameters["TargetSchema"],
                                                        bulkLoadParameters["TargetTable"],
                                                        Convert.ToBoolean(bulkLoadParameters["UseHeader"]),
                                                        Convert.ToInt32(bulkLoadParameters["HeaderRowsToSkip"]),
                                                        Convert.ToBoolean(bulkLoadParameters["Overwrite"]),
                                                        Convert.ToBoolean(bulkLoadParameters["Append"]),
                                                        Convert.ToInt32(bulkLoadParameters["BatchSize"]),
                                                        targetConnectionString,
                                                        Convert.ToInt32(bulkLoadParameters["DefaultColumnWidth"]),
                                                        bulkLoadParameters["NullValue"],
                                                        bulkLoadParameters["Comments"],
                                                        bulkLoadParameters["SchemaPath"],
                                                        bulkLoadParameters["ColumnFilter"],
                                                        bulkLoadParameters["QuoteIdentifier"].ToCharArray()[0],
                                                        bulkLoadParameters["EscapeCharacter"].ToCharArray()[0]);
            bl.Notifier += Notify;
            bl.LoadToSql();

            var schemaFile = Path.Combine(localFolderPath, "Schema.ini");
            if (File.Exists(schemaFile)) File.Delete(schemaFile);

            if (copyLocal)
            {
                Console.WriteLine("Deleting " + localFilePath);
                File.Delete(localFilePath);
                if (!Directory.EnumerateFileSystemEntries(localFolderPath).Any()) Directory.Delete(localFolderPath);                
            }
        }
        
        static string GetTargetConnectionString(Dictionary<string, string> bulkLoadParameters)
        {
            var targetConnectionString = ConfigurationManager.ConnectionStrings["TargetConnection"].ConnectionString;
            var targetServer = bulkLoadParameters["TargetServer"].ToString();
            if (targetServer != "")
            {
                targetConnectionString = System.Text.RegularExpressions.Regex.Replace(targetConnectionString, @"data source\=\w+", "Data Source=" + targetServer, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            }
            return targetConnectionString;
        }

        static void Notify(object sender, BulkInsertClass.NotifyEventArgs args)
        {
            Console.WriteLine(string.Format("{0}: {1}", args.TargetTable, args.Message));
        }

        static void ProcessFilesInDirectory(Dictionary<string, string> bulkLoadParameters, int maxDOP, bool copyLocal)
        {
            var inputFilePaths = Directory.GetFiles(bulkLoadParameters["InputFilePath"]).ToList<string>().Where(f => Path.GetExtension(f) == ".csv" || Path.GetExtension(f) == ".xlsx" || Path.GetExtension(f) == ".zip").ToList<string>();
            var bulkLoadQueue = new List<Dictionary<string, string>>();

            foreach (var inputFilePath in inputFilePaths)
            {
                var blp = new Dictionary<string, string>(bulkLoadParameters);
                blp["InputFilePath"] = inputFilePath;
                bulkLoadQueue.Add(blp);
            }

            ProcessFiles(bulkLoadQueue, copyLocal, maxDOP);
        }

        static void ProcessInputQueueFile(string inputQueueFilePath, int maxDOP, bool copyLocal)
        {
            Console.WriteLine("Input Queue File Specified -- Processing " + inputQueueFilePath);
            var existingTargetTables = new List<string>();
            var bulkLoadQueue = new List<Dictionary<string, string>>();

            var queueFile = new StreamReader(inputQueueFilePath);
            var headerLine = queueFile.ReadLine(); //header row is expected for queue files
            var queueParameters = GetQueueParametersFromHeader(headerLine);

            while (!queueFile.EndOfStream)
            {
                var bulkLoadParameters = GetDefaultParameters();
                bulkLoadParameters["InputFilePath"] = "";
                bulkLoadParameters["TargetTable"] = "";

                var line = queueFile.ReadLine().Split(',');
                for (int i = 0; i < line.Length; i ++)
                {
                    var queueParameter = queueParameters[i];
                    var parameterValue = line[i];
                    bulkLoadParameters[queueParameter] = parameterValue;
                }

                if (bulkLoadParameters["InputFilePath"] == "") continue;
                
                if (existingTargetTables.Contains(bulkLoadParameters["TargetTable"]) && bulkLoadParameters["TargetTable"] != "")
                {
                    bulkLoadParameters["Overwrite"] = "false";
                    bulkLoadParameters["Append"] = "true";
                }
                else
                {
                    existingTargetTables.Add(bulkLoadParameters["TargetTable"]);
                }

                bulkLoadQueue.Add(bulkLoadParameters);
            }
            queueFile.Close();

            Console.WriteLine(string.Format("Loading {0} files", bulkLoadQueue.Where(b => b["Overwrite"] == "true").Count()));
            
            //target tables to create, multi-threaded
            Parallel.ForEach(
                bulkLoadQueue.Where(b => b["Overwrite"] == "true"),
                new ParallelOptions { MaxDegreeOfParallelism = maxDOP },
                bulkLoadParameters => {
                    try
                    {
                        ProcessFile(bulkLoadParameters, copyLocal);
                    }
                    catch (Exception ex)
                    {
                        var errLog = new StreamWriter(bulkLoadParameters["InputFilePath"] + "_error.log", false);
                        errLog.WriteLine(bulkLoadParameters["InputFilePath"] + ": " + ex.Message + " at " + ex.StackTrace);
                        errLog.Close();
                    }   
                }
            );

            //target tables to append, single-threaded
            Console.WriteLine(string.Format("Appending {0} files", bulkLoadQueue.Where(b => b["Overwrite"] == "false").Count()));
            foreach (var bulkLoadParameters in bulkLoadQueue.Where(b => b["Overwrite"] == "false"))
            {                
                try
                {
                    ProcessFile(bulkLoadParameters, true);
                }
                catch (Exception ex)
                {
                    var errLog = new StreamWriter(bulkLoadParameters["InputFilePath"] + "_error.log", false);
                    errLog.WriteLine(bulkLoadParameters["InputFilePath"] + ": " + ex.Message + " at " + ex.StackTrace);
                    errLog.Close();
                }   
            }
        }
        
        static string[] GetQueueParametersFromHeader(string headerLine)
        {
            var queueParameters = headerLine.Split(',');
            var allowedParameters = new string[] { "InputFilePath", "Delimiter", "TargetDatabase", "TargetSchema", "TargetTable", "UseHeader", "HeaderRowsToSkip", "Overwrite", "Append", "BatchSize", "Comments" };
            var unrecognizedParameters = queueParameters.Except(allowedParameters);
            if (unrecognizedParameters.Any())
                throw new NotImplementedException(string.Format("Unrecognized parameters in queue file: {0}", string.Join(",", unrecognizedParameters)));
            return queueParameters;
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

        static Dictionary<string, string> GetDefaultParameters()
        {
            var bulkLoadParameters = new Dictionary<string, string>();
            foreach (var appSettingKey in ConfigurationManager.AppSettings.Keys)
                bulkLoadParameters.Add(appSettingKey.ToString(), ConfigurationManager.AppSettings[appSettingKey.ToString()]);
            return bulkLoadParameters;
        }

        static void GetFromFtp(string ftpPath)
        {
            // Get the object used to communicate with the server.  
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://www.contoso.com/test.htm");
            request.Method = WebRequestMethods.Ftp.DownloadFile;

            // This example assumes the FTP site uses anonymous logon.  
            request.Credentials = new NetworkCredential("anonymous", "janeDoe@contoso.com");

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();

            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            Console.WriteLine(reader.ReadToEnd());

            Console.WriteLine("Download Complete, status {0}", response.StatusDescription);

            reader.Close();
            response.Close();
        }
    }
}
