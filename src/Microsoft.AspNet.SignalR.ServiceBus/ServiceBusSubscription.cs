﻿// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Microsoft.AspNet.SignalR.ServiceBus
{
    internal class ServiceBusSubscription : IDisposable
    {
        private readonly NamespaceManager _namespaceManager;
        private readonly IList<TopicClient> _clients;
        private readonly ServiceBusScaleoutConfiguration _configuration;

        public readonly IList<SubscriptionContext> Subscriptions;

        public ServiceBusSubscription(ServiceBusScaleoutConfiguration configuration,
                                      NamespaceManager namespaceManager,
                                      IList<SubscriptionContext> subscriptions,
                                      IList<TopicClient> clients)
        {
            _configuration = configuration;
            _namespaceManager = namespaceManager;
            Subscriptions = subscriptions;
            _clients = clients;
        }

        public Task Publish(int topicIndex, Stream stream)
        {
            var message = new BrokeredMessage(stream, ownsStream: true)
            {
                TimeToLive = _configuration.TimeToLive
            };

            return _clients[topicIndex].SendAsync(message);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Disposing separately to avoid getting a lock on _clients and Subscriptions together

                lock (_clients)
                {
                    for (int i = 0; i < _clients.Count; i++)
                    {
                        _clients[i].Close();
                    }
                }

                lock (Subscriptions)
                {
                    for (int i = 0; i < Subscriptions.Count; i++)
                    {
                        var subscription = Subscriptions[i];
                        subscription.Receiver.Close();
                        _namespaceManager.DeleteSubscription(subscription.TopicPath, subscription.Name);

                    }
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
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
}
