using System;
using System.Data;

namespace ServiceBroker.Queues.Storage
{
    public class QueueActions
    {
        private Uri QueueUri { get; }
        private AbstractActions Actions { get; }

        public QueueActions(Uri queueUri, AbstractActions actions)
        {
            QueueUri = queueUri;
            Actions = actions;
        }

    
        public MessageEnvelope Dequeue()
        {
            MessageEnvelope message = null;
            Actions.ExecuteCommand("[SBQ].[Dequeue]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@queueName", QueueUri.ToServiceName());
                using (var reader = cmd.ExecuteReader(CommandBehavior.Default))
                {
                    if (!reader.Read())
                    {
                        message = null;
                        return;
                    }

                    message = Fill(reader);
                }
            });
            return message;
        }

        public void RegisterToSend(Uri destination, MessageEnvelope payload)
        {
            byte[] data = payload.Serialize();
            Actions.ExecuteCommand("[SBQ].[RegisterToSend]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@localServiceName", QueueUri.ToServiceName());
                cmd.Parameters.AddWithValue("@address", destination.ToServiceName());
                cmd.Parameters.AddWithValue("@route", string.Format("{0}://{1}", destination.Scheme, destination.Authority));
                cmd.Parameters.AddWithValue("@sizeOfData", payload.Data.Length);
                cmd.Parameters.AddWithValue("@deferProcessingUntilTime",
                                            (object)payload.DeferProcessingUntilUtcTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@sentAt", DateTime.UtcNow);
                cmd.Parameters.AddWithValue("@data", data);
                cmd.ExecuteNonQuery();
            });
        }

        private static MessageEnvelope Fill(IDataRecord reader)
        {
            var conversationId = reader.GetGuid(0);
        	var messageEnvelope = ((byte[]) reader.GetValue(1)).ToMessageEnvelope();
        	messageEnvelope.ConversationId = conversationId;
        	return messageEnvelope;
        }
    }
}