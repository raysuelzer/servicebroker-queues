using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

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


        internal static void ExecuteSqlScripts(string connectionStringName, string directory, Func<string, string> replacementFieldAction)
		{
			var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
			var connectionString = connectionStringSettings.ConnectionString;
			var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
			var databaseName = connectionStringBuilder.InitialCatalog;
			connectionStringBuilder.InitialCatalog = string.Empty;

			foreach (var file in Directory.GetFiles(directory, "*.sql").OrderBy(f => f))
			{
				var sql = File.ReadAllText(file);
				sql = sql.Replace("<databasename, sysname, queuedb>", databaseName);
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
		}
        internal static void ExecuteSqlScript(string connectionStringName, string sqlText)
        {
            ExecuteSqlScript(connectionStringName, sqlText, sql => sql);
        }

        internal static void ExecuteSqlScripts(string connectionStringName, string directory)
		{
			ExecuteSqlScripts(connectionStringName, directory, sql => sql);
		}
	}
}