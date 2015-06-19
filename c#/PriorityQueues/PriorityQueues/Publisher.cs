using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;

namespace PriorityQueues
{
    public class Publisher
    {
        private readonly string _broker;
        private readonly string _username;
        private readonly string _password;
        private readonly string _exchange;
        private readonly string _queue;
        private readonly string _bindingKey;
        private readonly long _maxQueuePriority;

        public Publisher(string broker, string username, string password, string exchange, string queue, string bindingKey, long maxQueuePriority)
        {
            _broker = broker;
            _username = username;
            _password = password;
            _exchange = exchange;
            _queue = queue;
            _bindingKey = bindingKey;
            _maxQueuePriority = maxQueuePriority;
        }

        public void Publish(Message msg)
        {
            var connectionFactory = new ConnectionFactory() { HostName = _broker, UserName = _username, Password = _password };
            using (var connection = connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(_exchange, "topic", true);
                    var arguments = new Dictionary<string, object> {{"x-max-priority", _maxQueuePriority}};
                    channel.QueueDeclare(_queue, true, false, false, arguments);
                    channel.QueueBind(_queue, _exchange, _bindingKey);
                    
                    var message = JsonConvert.SerializeObject(msg);
                    var basicProperties = new BasicProperties {Priority = msg.Priority};
                    channel.BasicPublish(_exchange, msg.RoutingKey, basicProperties, Encoding.UTF8.GetBytes(message));
                }
            }
        }
    }
}