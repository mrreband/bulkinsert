namespace BulkInsertClass
{
    public interface IBulkLoader
    {
        void LoadToSql();

        event EventHandler<NotifyEventArgs> Notifier;
    }
}
