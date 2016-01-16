using System;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceBroker.Queues.Storage;


namespace ServiceBroker.Queues.Tests
{
    [TestClass]
	public class CanUseQueue : QueueTest
    {
        private readonly Uri queueUri = new Uri("tcp://localhost:2204/h");
        private readonly QueueStorage qf;

        public CanUseQueue() : base("testqueue")
        {
            qf = new QueueStorage("testqueue");
            qf.Initialize();
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.CreateQueue(queueUri);
                actions.Commit();
            });
        }

        [TestMethod]
        public void CanPutSingleMessageInQueue()
        {
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 13, 12, 43, 5 },
                });
                actions.Commit();
            });
            Thread.Sleep(30);
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(queueUri);
                var message = queue.Dequeue();

                Assert.AreEqual(new byte[] { 13, 12, 43, 5 }, message.Data);
                actions.Commit();
            });
        }

        [TestMethod]
        public void WillGetMessagesBackInOrder()
        {
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 1 },
                });
                actions.Commit();
            });
            Thread.Sleep(10);
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 2 },
                });
                actions.Commit();
            });
            Thread.Sleep(10);
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 3 },
                });
                actions.Commit();
            });

            Thread.Sleep(300);
            MessageEnvelope m1 = null;
            MessageEnvelope m2 = null;
            MessageEnvelope m3 = null;

            qf.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(queueUri);
                m1 = queue.Dequeue();
                actions.Commit();
            });
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(queueUri);
                m2 = queue.Dequeue();
                actions.Commit();
            });
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(queueUri);
                m3 = queue.Dequeue();
                actions.Commit();
            });
            Assert.AreEqual(new byte[] { 1 }, m1.Data);
            Assert.AreEqual(new byte[] { 2 }, m2.Data);
            Assert.AreEqual(new byte[] { 3 }, m3.Data);
        }

        [TestMethod]
        public void WillNotGiveMessageToTwoClient()
        {
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 1 },
                });
                actions.Commit();
            });

            Thread.Sleep(30);
            qf.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(queueUri).RegisterToSend(queueUri, new MessageEnvelope
                {
                    Data = new byte[] { 2 },
                });
                actions.Commit();
            });

            Thread.Sleep(30);

            qf.Global(actions =>
            {
                actions.BeginTransaction();
                var m1 = actions.GetQueue(queueUri).Dequeue();
                MessageEnvelope m2 = null;

                qf.Global(queuesActions =>
                {
                    queuesActions.BeginTransaction();
                    m2 = queuesActions.GetQueue(queueUri).Dequeue();

                    queuesActions.Commit();
                });
                Assert.IsTrue(m2 == null || (m2.Data != m1.Data));
                actions.Commit();
            });
        }

        [TestMethod]
        public void WillGiveNullWhenNoItemsAreInQueue()
        {
            qf.Global(actions =>
            {
                var message = actions.GetQueue(queueUri).Dequeue();
                Assert.IsNull(message);
                actions.Commit();
            });
        }
    }
}