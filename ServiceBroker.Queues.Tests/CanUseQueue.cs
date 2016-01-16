using System;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceBroker.Queues.Storage;


namespace ServiceBroker.Queues.Tests
{
    [TestClass]
	public class CanUseQueue : QueueTest
    {
        private Uri QueueUri => new Uri("tcp://localhost:2204/h");
        
        private QueueStorage QueueStorage { get; }

        public CanUseQueue() : base("testqueue")
        {
            QueueStorage = new QueueStorage("testqueue");
            QueueStorage.Initialize();
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.CreateQueue(QueueUri);
                actions.Commit();
            });
        }

        [TestMethod]
        public void CanPutSingleMessageInQueue()
        {
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 13, 12, 43, 5 },
                });
                actions.Commit();
            });
            Thread.Sleep(30);
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(QueueUri);
                var message = queue.Dequeue();
                Assert.AreEqual("1312435", string.Join("", message.Data.Select(b => b.ToString())));
                actions.Commit();
            });
        }

        [TestMethod]
        public void WillGetMessagesBackInOrder()
        {
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 1 },
                });
                actions.Commit();
            });
            Thread.Sleep(10);
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 2 },
                });
                actions.Commit();
            });
            Thread.Sleep(10);
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 3 },
                });
                actions.Commit();
            });

            Thread.Sleep(300);
            MessageEnvelope m1 = null;
            MessageEnvelope m2 = null;
            MessageEnvelope m3 = null;

            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(QueueUri);
                m1 = queue.Dequeue();
                actions.Commit();
            });
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(QueueUri);
                m2 = queue.Dequeue();
                actions.Commit();
            });
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(QueueUri);
                m3 = queue.Dequeue();
                actions.Commit();
            });
            Assert.AreEqual(1, m1.Data[0]);
            Assert.AreEqual(2, m2.Data[0]);
            Assert.AreEqual(3, m3.Data[0]);
        }

        [TestMethod]
        public void WillNotGiveMessageToTwoClient()
        {
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 1 },
                });
                actions.Commit();
            });

            Thread.Sleep(30);
            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, new MessageEnvelope
                {
                    Data = new byte[] { 2 },
                });
                actions.Commit();
            });

            Thread.Sleep(30);

            QueueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var m1 = actions.GetQueue(QueueUri).Dequeue();
                MessageEnvelope m2 = null;

                QueueStorage.Global(queuesActions =>
                {
                    queuesActions.BeginTransaction();
                    m2 = queuesActions.GetQueue(QueueUri).Dequeue();

                    queuesActions.Commit();
                });
                Assert.IsTrue(m2 == null || (m2.Data != m1.Data));
                actions.Commit();
            });
        }

        [TestMethod]
        public void WillGiveNullWhenNoItemsAreInQueue()
        {
            QueueStorage.Global(actions =>
            {
                var message = actions.GetQueue(QueueUri).Dequeue();
                Assert.IsNull(message);
                actions.Commit();
            });
        }
    }
}