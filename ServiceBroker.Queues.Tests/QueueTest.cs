#define QUEUE_MODIFY

using System;
using System.Configuration;
using ServiceBroker.Queues.Storage;
using System.Data.SqlClient;
using System.Data;

namespace ServiceBroker.Queues.Tests
{

    public abstract class QueueTest
	{
		protected QueueTest(string connectionStringName)
		{
			var connectionStringBuilder =
				new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString);
			var databaseName = connectionStringBuilder.InitialCatalog;
			connectionStringBuilder.InitialCatalog = "master";
			using(var connection = new SqlConnection(connectionStringBuilder.ConnectionString))
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
			catch(SqlException ex)
            {
                new SchemaCreator().Create(connectionStringName, 2204);
            }
			StorageUtil.PurgeAll(connectionStringName);
		}

	}
}