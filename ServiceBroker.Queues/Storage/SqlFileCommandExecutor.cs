using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
	public static class SqlFileCommandExecutor
	{
	    internal static void ExecuteSqlScript(string connectionStringName, string sqlText,
	        Func<string, string> replacementFieldAction)
	    {
            var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
            var connectionString = connectionStringSettings.ConnectionString;
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = string.Empty;

            var sql = sqlText.Replace("<databasename, sysname, queuedb>", databaseName);
            sql = replacementFieldAction(sql);
            var sqlStatements = sql.Split(new[] { "GO" }, StringSplitOptions.None);
            using (var localConnection = new SqlConnection(connectionStringBuilder.ConnectionString))
            {
                if (localConnection.State == ConnectionState.Closed)
                    localConnection.Open();
                foreach (var commandText in sqlStatements)
                {
                    using (var cmd = new SqlCommand(commandText, localConnection))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

        }
     
        internal static void ExecuteSqlScript(string connectionStringName, string sqlText)
        {
            ExecuteSqlScript(connectionStringName, sqlText, sql => sql);
        }
	}
}