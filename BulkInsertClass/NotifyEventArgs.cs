using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertClass
{
    public class NotifyEventArgs : EventArgs
    {
        public required string Message { get; set; }
        public required string InputFilePath { get; set; }
        public required string TargetTable { get; set; }
    }
}
