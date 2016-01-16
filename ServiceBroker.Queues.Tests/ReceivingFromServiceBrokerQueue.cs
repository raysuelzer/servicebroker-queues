using System;
using System.Text;
using System.Threading;
using System.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ServiceBroker.Queues.Tests
{
    [TestClass]
    public class ReceivingFromServiceBrokerQueue : QueueTest, IDisposable
    {
        private QueueManager QueueManager { get; }

        private Uri QueueUri => new Uri("tcp://localhost:2204/h");


        public ReceivingFromServiceBrokerQueue() : base("testqueue")
        {
            QueueManager = new QueueManager("testqueue");
            QueueManager.CreateQueues(QueueUri);
        }

        public void Dispose()
        {
            QueueManager.Dispose();
        }

    	[TestMethod]
        public void CanReceiveFromQueue()
        {
            using (var tx = new TransactionScope())
            {
                QueueManager.Send(QueueUri, QueueUri,
                                   new MessageEnvelope
                                   {
                                       Data = Encoding.Unicode.GetBytes("hello"),
                                   });
                tx.Complete();
            }
            Thread.Sleep(50);
            using(var tx = new TransactionScope())
            {
                var message = QueueManager.GetQueue(QueueUri).Receive();
                Assert.AreEqual("hello", Encoding.Unicode.GetString(message.Data));
                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = QueueManager.GetQueue(QueueUri).Receive();
                Assert.IsNull(message);
                tx.Complete();
            }
        }

        [TestMethod]
        public void WhenRevertingTransactionMessageGoesBackToQueue()
        {
            using (var tx = new TransactionScope())
            {
                QueueManager.Send(QueueUri, QueueUri,
                                   new MessageEnvelope
                                   {
                                       Data = Encoding.Unicode.GetBytes("hello"),
                                   });
                tx.Complete();
            }
            Thread.Sleep(30);

            using (new TransactionScope())
            {
                var message = QueueManager.GetQueue(QueueUri).Receive();
                Assert.AreEqual("hello", Encoding.Unicode.GetString(message.Data));
            }
            using (new TransactionScope())
            {
                var message = QueueManager.GetQueue(QueueUri).Receive();
                Assert.AreEqual("hello", Encoding.Unicode.GetString(message.Data));
            }
        }
    }
}