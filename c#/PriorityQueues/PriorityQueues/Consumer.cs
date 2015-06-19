using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;

namespace PriorityQueues
{
    public class Consumer
    {
        private readonly string _broker;
        private readonly string _username;
        private readonly string _password;
        private readonly string _exchange;
        private readonly string _queue;
        private readonly string _bindingKey;
        private readonly long _maxQueuePriority;

        public Consumer(string broker, string username, string password, string exchange, string queue, string bindingKey, long maxQueuePriority)
        {
            _broker = broker;
            _username = username;
            _password = password;
            _exchange = exchange;
            _queue = queue;
            _bindingKey = bindingKey;
            _maxQueuePriority = maxQueuePriority;
        }

        public void Consume(long numMsgs)
        {
            var connectionFactory = new ConnectionFactory() { HostName = _broker, UserName = _username, Password = _password };
            using (var connection = connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.BasicQos(0, 1, true);
                    channel.ExchangeDeclare(_exchange, "topic", true);
                    var arguments = new Dictionary<string, object> {{"x-max-priority", _maxQueuePriority}};
                    channel.QueueDeclare(_queue, true, false, false, arguments);
                    channel.QueueBind(_queue, _exchange, _bindingKey);

                    var msgCount = 0;
                    while (msgCount < numMsgs)
                    {
                        var messageDelivery = channel.BasicGet(_queue, false);
                        if (messageDelivery == null) continue;
                        channel.BasicAck(messageDelivery.DeliveryTag, false);
                        var messageJson = Encoding.UTF8.GetString(messageDelivery.Body);
                        Console.WriteLine("Processing {0}...", messageJson);
                        var message = JsonConvert.DeserializeObject<Message>(messageJson);
                        Thread.Sleep((int) (message.Payload * 1000));
                        msgCount ++;
                    }

                }
            }
        }
    }
}