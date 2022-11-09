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
            var bulkLoadParameters = GetDefaultParameters(args);            
            var copyLocal = Convert.ToBoolean(bulkLoadParameters["CopyLocal"]);

            var targetConnectionString = ConfigurationManager.ConnectionStrings["TargetConnection"].ConnectionString;
            var bulkLoadRequest = new BulkInsertRequest(bulkLoadParameters, targetConnectionString);
            bulkLoadRequest.ProcessRequest();

            Console.WriteLine("done");
            Environment.Exit(0);
        }

        static Dictionary<string, string> GetDefaultParameters(string[] args)
        {
            var bulkLoadParameters = new Dictionary<string, string>();
            foreach (var appSettingKey in ConfigurationManager.AppSettings.Keys)
                bulkLoadParameters.Add(appSettingKey.ToString(), ConfigurationManager.AppSettings[appSettingKey.ToString()]);

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
