using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Configuration;

namespace BatchLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            var bulkLoadParameters = GetDefaultParameters(args);
            var inputFolder = bulkLoadParameters["InputFolder"];
            var copyLocal = Convert.ToBoolean(bulkLoadParameters["CopyLocal"]);
            var targetConnectionString = ConfigurationManager.ConnectionStrings["TargetConnection"].ConnectionString;

            //cap max degrees of parallelism between 1 and 8
            var maxDOP = Convert.ToInt32(ConfigurationManager.AppSettings["MaxDOP"].ToString());
            maxDOP = (maxDOP < 1) ? 1 : (maxDOP > 8) ? 8 : maxDOP;

            var inputFilePaths = Directory.GetFiles(inputFolder).ToList();
            ProcessFiles(inputFilePaths, bulkLoadParameters, targetConnectionString, maxDOP);

            Console.WriteLine("done");
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
                        bulkLoadParameters["InputFilePath"] = inputFilePath;
                        var bulkLoadRequest = new BulkInsert.BulkInsertRequest(bulkLoadParameters, targetConnectionString);
                        bulkLoadRequest.ProcessRequest();
                    }
                    catch (Exception ex)
                    {
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
