using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertClass
{
    public interface IBulkLoader
    {
        void LoadToSql();

        event EventHandler<NotifyEventArgs> Notifier;
    }
}
