using System.Threading;

namespace PriorityQueues
{
    public class Driver
    {
        private const uint NumMsgs = 10;
        private static readonly PriorityQueuesConfig Config = new PriorityQueuesConfig();

        public static void Main()
        {
            ParallelPublishConsume();
        }

        private static void ParallelPublishConsume()
        {
            var consumer1 = new Thread(Consume);
            var publisher1 = new Thread(Publish);

            var consumer2 = new Thread(Consume);
            var publisher2 = new Thread(Publish);

            consumer1.Start();
            consumer2.Start();
            
            publisher1.Start();
            publisher2.Start();

            consumer1.Join();
            consumer2.Join();
            
            publisher1.Join();
            publisher2.Join();
        }

        private static void SequentialPublishConsume()
        {
            Publish();
            Consume();
        }

        private static void Consume()
        {
            var consumer = new Consumer(Config.Broker, Config.Username, Config.Password, Config.Exchange, Config.Queue,
                Config.BindingKey, Config.MaxQueuePriority);
            consumer.Consume(NumMsgs);
        }

        private static void Publish()
        {
            var publisher = new Publisher(Config.Broker, Config.Username, Config.Password, Config.Exchange, Config.Queue,
                Config.BindingKey, Config.MaxQueuePriority);
            var routingKeys = new[] {"req.fc", "req.opt"};
            for (byte i = 1; i <= NumMsgs; i++)
            {
                publisher.Publish(new Message(5, i, routingKeys[i%2]));
            }
        }
    }
}