using System;

namespace ServiceBroker.Queues
{
    public class Queue : IQueue
    {
        private QueueManager QueueManager { get; }
        private Uri QueueUri { get; }

        public Queue(QueueManager queueManager, Uri queueUri)
        {
            QueueManager = queueManager;
            QueueUri = queueUri;
        }

        public MessageEnvelope Receive()
        {
            return QueueManager.Receive(QueueUri);
        }

        public MessageEnvelope Receive(TimeSpan timeout)
        {
            return QueueManager.Receive(QueueUri, timeout);
        }
    }
}