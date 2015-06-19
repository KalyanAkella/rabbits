namespace PriorityQueues
{
    public class Message
    {
        public Message(long payload, byte priority, string routingKey)
        {
            Payload = payload;
            Priority = priority;
            RoutingKey = routingKey;
        }

        public long Payload { get; private set; }
        public byte Priority { get; private set; }
        public string RoutingKey { get; private set; }
    }
}
