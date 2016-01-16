using System;
using System.Data;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
    public abstract class AbstractActions
    {
        protected SqlConnection Connection { get; }
        protected SqlTransaction Transaction;

        protected AbstractActions(SqlConnection connection)
        {
            Connection = connection;
        }

        public QueueActions GetQueue(Uri queueUri)
        {
            return new QueueActions(queueUri, this);
        }

        public void BeginTransaction()
        {
            Transaction = Connection.BeginTransaction(IsolationLevel.RepeatableRead);
        }

        public void Commit()
        {
            if(Transaction == null)
                return;
            Transaction.Commit();
        }

        internal void ExecuteCommand(string commandText, Action<SqlCommand> command)
        {
            using(var sqlCommand = new SqlCommand(commandText, Connection))
            {
                sqlCommand.Transaction = Transaction;
                command(sqlCommand);
            }
        }
    }
}