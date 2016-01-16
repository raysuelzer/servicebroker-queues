using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceBroker.Queues.Storage;

namespace ServiceBroker.Queues.Tests
{
    [TestClass]
    public class UtilityTests
    {
        [TestMethod]
        public void CanCorrectlyParseUriIntoServiceName()
        {
            var uri = new Uri("tcp://something.fake.com:2204/corey/devqueue");
            Assert.AreEqual(uri.ToServiceName(), "something.fake.com:2204/corey/devqueue");
        }
    }
}