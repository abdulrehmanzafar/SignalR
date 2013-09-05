using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Microsoft.AspNet.SignalR.ServiceBus
{
    public class ServiceBusConnectionContext : IDisposable
    {
        private readonly NamespaceManager _namespaceManager;
        private readonly ServiceBusScaleoutConfiguration _configuration;

        private readonly SubscriptionContext[] Subscriptions;
        private readonly TopicClient[] TopicClients;

        public object SubscriptionsLock;
        public object TopicClientsLock;
        public readonly IList<string> TopicNames;
        public readonly Action<int, IEnumerable<BrokeredMessage>> Handler;
        public readonly Action<int, Exception> ErrorHandler;

        public bool IsConnectionContextDisposed;

        public ServiceBusConnectionContext(ServiceBusScaleoutConfiguration configuration, NamespaceManager namespaceManager, IList<string> topicNames, Action<int, IEnumerable<BrokeredMessage>> handler, Action<int, Exception> errorHandler)
        {
            _namespaceManager = namespaceManager;
            _configuration = configuration;

            Subscriptions = new SubscriptionContext[topicNames.Count];
            TopicClients = new TopicClient[topicNames.Count];
            
            TopicNames = topicNames;
            Handler = handler;
            ErrorHandler = errorHandler;
            
            IsConnectionContextDisposed = false;

            TopicClientsLock = new object();
            SubscriptionsLock = new object();
        }

        public Task Publish(int topicIndex, Stream stream)
        {
            var message = new BrokeredMessage(stream, ownsStream: true)
            {
                TimeToLive = _configuration.TimeToLive
            };

            return TopicClients[topicIndex].SendAsync(message);
        } 

        public void UpdateSubscriptionContext(SubscriptionContext subscriptionContext, int topicIndex)
        {
            if (!IsConnectionContextDisposed)
            {
                Subscriptions[topicIndex] = subscriptionContext;
            }
        }

        public void UpdateTopicClients(TopicClient topicClient, int topicIndex)
        {
            if (!IsConnectionContextDisposed)
            {
                TopicClients[topicIndex] = topicClient;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!IsConnectionContextDisposed)
                {
                    lock (TopicClientsLock)
                    {
                        lock (SubscriptionsLock)
                        {
                            for (int i = 0; i < TopicNames.Count; i++)
                            {
                                TopicClients[i].Close();
                                SubscriptionContext subscription = Subscriptions[i];
                                subscription.Receiver.CloseAsync();
                                _namespaceManager.DeleteSubscription(subscription.TopicPath, subscription.Name);
                            }

                            IsConnectionContextDisposed = true;
                        }
                    }
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }

    public class SubscriptionContext
    {
        public string TopicPath;
        public string Name;
        public MessageReceiver Receiver;

        public SubscriptionContext(string topicPath, string subName, MessageReceiver receiver)
        {
            TopicPath = topicPath;
            Name = subName;
            Receiver = receiver;
        }
    }
}
