using System;
using System.Configuration;
using System.Data;
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

        public void CreateIfNotExists(string connectionStringName, int port)
        {
            var connectionStringBuilder =
                new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString);
            var databaseName = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = "master";
            using (var connection = new SqlConnection(connectionStringBuilder.ConnectionString))
            {
                bool databaseExists = false;
                connection.Open();
                using (var dbExistsCommand = new SqlCommand($"SELECT DB_ID ('{databaseName}')", connection))
                {
                    dbExistsCommand.CommandType = CommandType.Text;
                    var result = dbExistsCommand.ExecuteScalar();
                    databaseExists = !String.IsNullOrEmpty(result.ToString());
                }
                //Create database if it does not exist
                if (!databaseExists)
                {
                    using (var createCommand = new SqlCommand(
                        @"				
				BEGIN
					CREATE DATABASE [<databasename, sysname, queuedb>]
				END".Replace("<databasename, sysname, queuedb>", databaseName), connection))
                    {
                        createCommand.CommandType = CommandType.Text;

                        createCommand.ExecuteNonQuery();
                    }
                }


            }
            try
            {
                var storage = new QueueStorage(connectionStringName);
                storage.Initialize();
            }
            catch (SqlException ex)
            {
                Debug.WriteLine(ex);
                new SchemaCreator().Create(connectionStringName, 2204);
            }
        }
    }
}