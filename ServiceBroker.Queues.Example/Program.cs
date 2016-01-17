using System;
using System.Text;
using Newtonsoft.Json;
using ServiceBroker.Queues.Storage;

namespace ServiceBroker.Queues.Example
{
    class Program
    {
        // Our broker endpoint
        private static Uri QueueUri => new Uri("tcp://localhost:2204/h");
        
     
        static void Main(string[] args)
        {
            Console.WriteLine("Creating schema and database if it doesn't exist.");

            var schemaCreator = new SchemaCreator();
            schemaCreator.CreateIfNotExists("testqueue", 2204);

            Console.WriteLine("Creating queue storage instance");
            var queueStorage = new QueueStorage("testqueue");

            queueStorage.Initialize();
            queueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.CreateQueue(QueueUri);
                actions.Commit();
            });
            

            Console.WriteLine("Type your favorite color...");
            var userColorInput = Console.ReadLine();

            //Add a message to the queue
            var colorDto = new FavoriteColorMessage()
            {
                Color = userColorInput
            };

            Console.WriteLine("Sending color message");
            queueStorage.Global(actions =>
            {
                var envelope = new MessageEnvelope
                {
                    Data = PocoToJsonByteArray(colorDto)
                };
                envelope.Headers.Add("MessageType", nameof(FavoriteColorMessage));


                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, envelope);
                actions.Commit();
            });
            
            Console.WriteLine("Type your favorite animal...");
            var userAnimalInput = Console.ReadLine();

            //Add a message to the queue
            var animalDto = new FavoriteAnimalMessage()
            {
                Animal = userAnimalInput
            };

            Console.WriteLine("Sending animal message");


            queueStorage.Global(actions =>
            {
                var envelope = new MessageEnvelope
                {
                    Data = PocoToJsonByteArray(animalDto)
                };
                envelope.Headers.Add("MessageType", nameof(FavoriteAnimalMessage));
                
                actions.BeginTransaction();
                actions.GetQueue(QueueUri).RegisterToSend(QueueUri, envelope);
                actions.Commit();
            });

            Console.WriteLine("Press enter to retrieve your messages from the queue");
            Console.ReadLine();

            //retrieve messages from the queue
            RetrieveAllMessages(queueStorage);

            Console.ReadLine();
        }


        public static void RetrieveAllMessages(QueueStorage queueStorage)
        {
            var messagesExist = true;
            while (messagesExist)
            queueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                var queue = actions.GetQueue(QueueUri);
                var message = queue.Dequeue();
                if (message == null)
                {
                    // there are no more messages, end the loop.
                    messagesExist = false;
                    return;
                }
                try
                {
                    HandleMessage(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    //message will be marked as handled
                    actions.Commit();
                }

            });
        }

        public static void HandleMessage(MessageEnvelope messageEnvelope)
        {
            var messagetypeValue = messageEnvelope.Headers.Get("MessageType");
            switch (messagetypeValue)
            {
                case nameof(FavoriteAnimalMessage):
                    var animalDto = DeserializeJsonByteArray<FavoriteAnimalMessage>(messageEnvelope.Data);
                    Console.WriteLine("Your favorite animal is " + animalDto.Animal);
                    return;
                case nameof(FavoriteColorMessage):
                    var colorDto = DeserializeJsonByteArray<FavoriteColorMessage>(messageEnvelope.Data);
                    Console.WriteLine("Your favorite color is " + colorDto.Color);
                    return;
                default:
                    return;
            }
        }


        private static byte[] PocoToJsonByteArray(object poco)
        {
            var jsonString = JsonConvert.SerializeObject(poco);
            return System.Text.Encoding.UTF8.GetBytes(jsonString);
        }

        private static T DeserializeJsonByteArray<T>(byte[] bytes)
        {
            var jsonString = Encoding.UTF8.GetString(bytes);
            return JsonConvert.DeserializeObject<T>(jsonString);
        }

    }
}
