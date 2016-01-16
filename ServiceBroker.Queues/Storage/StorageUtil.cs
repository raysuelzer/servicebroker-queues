using System.Diagnostics;
using ServiceBroker.Queues.ServiceBroker.SQL;

namespace ServiceBroker.Queues.Storage
{
    public class StorageUtil
    {
		[Conditional("QUEUE_MODIFY")]
        public static void PurgeAll(string connectionStringName)
        {
			SqlFileCommandExecutor.ExecuteSqlScript(connectionStringName, SqlCommands.PurgeAllHistory);
        }
    }
}