using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertClass
{
    public class NotifyEventArgs : EventArgs
    {
        public string Message { get; set; }
        public string InputFilePath { get; set; }
        public string TargetTable { get; set; }
    }
}
