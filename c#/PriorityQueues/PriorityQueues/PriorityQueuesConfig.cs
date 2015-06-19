using System;
using System.Collections.Specialized;
using System.Configuration;

namespace PriorityQueues
{
    public class PriorityQueuesConfig
    {
        private readonly NameValueCollection _settings;

        public PriorityQueuesConfig()
        {
            _settings = ConfigurationManager.AppSettings;
        }

        public string Broker
        {
            get { return _settings["broker"]; }
        }

        public string Username
        {
            get { return _settings["username"]; }
        }

        public string Password
        {
            get { return _settings["password"]; }
        }

        public string Exchange
        {
            get { return _settings["exchange"]; }
        }

        public string Queue
        {
            get { return _settings["queue"]; }
        }

        public string BindingKey
        {
            get { return _settings["binding-key"]; }
        }

        public long MaxQueuePriority
        {
            get { return Convert.ToInt64(_settings["max-queue-priority"]); }
        }
    }
}