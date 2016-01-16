using System.Data.SqlClient;
using System.Diagnostics;
using ServiceBroker.Queues.ServiceBroker.SQL;

namespace ServiceBroker.Queues.Storage
{
    public class SchemaCreator
    {
        public static string SchemaVersion
        {
            get { return "1.0"; }
        }

		[Conditional("QUEUE_MODIFY")]
        public void Create(string connectionStringName, int port)
        {
			SqlFileCommandExecutor.ExecuteSqlScript(connectionStringName, SqlCommands.CreateQueueSchema,
				sql =>
				{
					sql = sql.Replace("<port, , 2204>", port.ToString());
					return sql;
				});
           
            SqlConnection.ClearAllPools();
        }
    }
}