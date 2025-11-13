using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Configuration;
using BulkInsertClass;
using Newtonsoft.Json.Linq;

namespace BatchLoader
{
    class Program
    {
        static bool IsSupportedFile(String filePath) {
            var supportedExtensions = BulkLoaderFactory.GetSupportedExtensions();
            var fileExtension = Path.GetExtension(filePath).ToUpper();
            return supportedExtensions.Contains(fileExtension);
        }

        static void Main(string[] args)
        {
            var bulkLoadParameters = GetDefaultParameters(args);
            var targetConnectionString = ConfigurationManager.ConnectionStrings["TargetConnection"].ConnectionString;

            //cap max degrees of parallelism between 1 and 8
            var maxDOP = Convert.ToInt32(ConfigurationManager.AppSettings["MaxDOP"].ToString());
            maxDOP = (maxDOP < 1) ? 1 : (maxDOP > 8) ? 8 : maxDOP;

            var inputFilePaths = GetInputFiles(bulkLoadParameters);
            ProcessFiles(inputFilePaths, bulkLoadParameters, targetConnectionString, maxDOP);

            Console.WriteLine("done");
        }

        static List<String> GetInputFiles(Dictionary<string, string> bulkLoadParameters) {
            var inputFolder = bulkLoadParameters["InputFolder"];
            var fileFilter = bulkLoadParameters.TryGetValue("FileFilter", out var value) ? value : "*";
            var recursive = bulkLoadParameters.TryGetValue("Recursive", out var _recursive) ? Convert.ToBoolean(_recursive): false;
            var fileExtensionOverride = bulkLoadParameters["FileExtensionOverride"];

            var enumerationOptions = new EnumerationOptions();
            enumerationOptions.RecurseSubdirectories = recursive;

            var inputFilePaths = Directory.GetFiles(inputFolder, fileFilter, enumerationOptions).ToList();
            if (fileExtensionOverride == "")
            {
                inputFilePaths = inputFilePaths.Where(x => IsSupportedFile(x) == true).ToList();
            }
            return inputFilePaths;
        }

        static void ProcessFiles(List<string> InputFilePaths, Dictionary<string, string> bulkLoadParameters, String targetConnectionString, int maxDOP)
        {
            Parallel.ForEach(
                InputFilePaths,
                new ParallelOptions { MaxDegreeOfParallelism = maxDOP },
                inputFilePath =>
                {
                    try
                    {
                        var newDictionary = bulkLoadParameters.ToDictionary(entry => entry.Key, entry => entry.Value);
                        newDictionary["InputFilePath"] = inputFilePath;
                        var bulkLoadRequest = new BulkInsert.BulkInsertRequest(newDictionary, targetConnectionString);
                        bulkLoadRequest.ProcessRequest();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error: " + inputFilePath + ": " + ex.Message + " at " + ex.StackTrace);
                        var errLog = new StreamWriter("err.log", false);
                        errLog.WriteLine(inputFilePath + ": " + ex.Message + " at " + ex.StackTrace);
                        errLog.Close();
                    }
                }
            );
        }


        static Dictionary<string, string> GetDefaultParameters(string[] args)
        {
            //load from config
            var bulkLoadParameters = new Dictionary<string, string>();
            foreach (var appSettingKey in ConfigurationManager.AppSettings.Keys)
                bulkLoadParameters.Add(appSettingKey.ToString(), ConfigurationManager.AppSettings[appSettingKey.ToString()]);

            //user overrides
            foreach (string s in args)
            {
                var keyValuePair = s.Trim().Split('=');
                if (keyValuePair.Count() != 2) throw new ArgumentException("Improper Argument Format (should be key=value): " + s);
                if (!bulkLoadParameters.ContainsKey(keyValuePair[0])) throw new ArgumentException("Argument not supported: " + s);
                Console.WriteLine(s);
                bulkLoadParameters[keyValuePair[0]] = keyValuePair[1];
            }

            return bulkLoadParameters;
        }

    }

}
