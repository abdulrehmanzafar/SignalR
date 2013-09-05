﻿// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Microsoft.AspNet.SignalR.ServiceBus
{
    internal class ServiceBusConnection : IDisposable
    {
        private const int DefaultReceiveBatchSize = 1000;
        private static readonly TimeSpan BackoffAmount = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan ErrorBackOffAmount = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan DefaultReadTimeout = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan ErrorReadTimeout = TimeSpan.FromSeconds(0.5);
        private static readonly TimeSpan IdleSubscriptionTimeout = TimeSpan.FromHours(1);
        private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(10);


        private readonly NamespaceManager _namespaceManager;
        private readonly MessagingFactory _factory;
        private readonly ServiceBusScaleoutConfiguration _configuration;
        private readonly TraceSource _trace;

        public ServiceBusConnection(ServiceBusScaleoutConfiguration configuration, TraceSource traceSource)
        {
            _trace = traceSource;

            try
            {
                _namespaceManager = NamespaceManager.CreateFromConnectionString(configuration.ConnectionString);
            }
            catch (ConfigurationErrorsException ex)
            {
                _trace.TraceError("Invalid connection string '{0}': {1}", configuration.ConnectionString, ex.Message);

                throw;
            }

            _factory = MessagingFactory.CreateFromConnectionString(configuration.ConnectionString);
            _factory.RetryPolicy = RetryExponential.Default;
            _configuration = configuration;
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "The disposable is returned to the caller")]
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "We retry to create the topics on exceptions")]
        public ServiceBusConnectionContext Subscribe(IList<string> topicNames,
                                                Action<int, IEnumerable<BrokeredMessage>> handler,
                                                Action<int, Exception> errorHandler)
        {
            if (topicNames == null)
            {
                throw new ArgumentNullException("topicNames");
            }

            if (handler == null)
            {
                throw new ArgumentNullException("handler");
            }

            _trace.TraceInformation("Subscribing to {0} topic(s) in the service bus...", topicNames.Count);

            var connectionContext = new ServiceBusConnectionContext(_configuration, _namespaceManager, topicNames, handler, errorHandler);

            for (var topicIndex = 0; topicIndex < topicNames.Count; ++topicIndex)
            {
                Retry(() => CreateTopic(connectionContext, topicIndex));
            }

            _trace.TraceInformation("Subscription to {0} topics in the service bus Topic service completed successfully.", topicNames.Count);

            return connectionContext;
        }

        private void CreateTopic(ServiceBusConnectionContext connectionContext, int topicIndex)
        {
            lock (connectionContext.TopicClientsLock)
            {
                if (connectionContext.IsConnectionContextDisposed)
                {
                    return;
                }

                string topicName = connectionContext.TopicNames[topicIndex];

                if (!_namespaceManager.TopicExists(topicName))
                {
                    try
                    {
                        _trace.TraceInformation("Creating a new topic {0} in the service bus...", topicName);

                        _namespaceManager.CreateTopic(topicName);

                        _trace.TraceInformation("Creation of a new topic {0} in the service bus completed successfully.", topicName);
                    }
                    catch (MessagingEntityAlreadyExistsException)
                    {
                        // The entity already exists
                        _trace.TraceInformation("Creation of a new topic {0} threw an MessagingEntityAlreadyExistsException.", topicName);
                    }
                }

                // Create a client for this topic
                connectionContext.UpdateTopicClients(TopicClient.CreateFromConnectionString(_configuration.ConnectionString, topicName), topicIndex);

                _trace.TraceInformation("Creation of a new topic client {0} completed successfully.", topicName);
            }

            CreateSubscription(connectionContext, topicIndex);
        }

        private void CreateSubscription(ServiceBusConnectionContext connectionContext, int topicIndex)
        {
            lock (connectionContext.SubscriptionsLock)
            {
                if (connectionContext.IsConnectionContextDisposed)
                {
                    return;
                }

                string topicName = connectionContext.TopicNames[topicIndex];

                // Create a random subscription
                string subscriptionName = Guid.NewGuid().ToString();

                try
                {
                    var subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName);

                    // This cleans up the subscription while if it's been idle for more than the timeout.
                    subscriptionDescription.AutoDeleteOnIdle = IdleSubscriptionTimeout;

                    _namespaceManager.CreateSubscription(subscriptionDescription);

                    _trace.TraceInformation("Creation of a new subscription {0} for topic {1} in the service bus completed successfully.", subscriptionName, topicName);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    // The entity already exists
                    _trace.TraceInformation("Creation of a new subscription {0} for topic {1} threw an MessagingEntityAlreadyExistsException.", subscriptionName, topicName);
                }

                // Create a receiver to get messages
                string subscriptionEntityPath = SubscriptionClient.FormatSubscriptionPath(topicName, subscriptionName);
                MessageReceiver receiver = _factory.CreateMessageReceiver(subscriptionEntityPath, ReceiveMode.ReceiveAndDelete);

                _trace.TraceInformation("Creation of a message receive for subscription entity path {0} in the service bus completed successfully.", subscriptionEntityPath);

                connectionContext.UpdateSubscriptionContext(new SubscriptionContext(topicName, subscriptionName, receiver), topicIndex);

                var receiverContext = new ReceiverContext(topicIndex, receiver, connectionContext);

                ProcessMessages(receiverContext);
            }
        }

        private void Retry(Action action)
        {
            while (true)
            {
                try
                {
                    action();
                    break;
                }
                catch (UnauthorizedAccessException ex)
                {
                    _trace.TraceError("Failed to create service bus subscription or topic : {0}", ex.Message);
                    throw;
                }
                catch (QuotaExceededException ex)
                {
                    _trace.TraceError("Failed to create service bus subscription or topic : {0}", ex.Message);
                    throw;
                }
                catch (MessagingException ex)
                {
                    _trace.TraceError("Failed to create service bus subscription or topic : {0}", ex.Message);
                    if (ex.IsTransient)
                    {
                        Thread.Sleep(RetryDelay);
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (Exception ex)
                {
                    _trace.TraceError("Failed to create service bus subscription or topic : {0}", ex.Message);
                    Thread.Sleep(RetryDelay);
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Close the factory
                _factory.Close();
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are handled through the error handler callback")]
        private void ProcessMessages(ReceiverContext receiverContext)
        {
        receive:

            try
            {
                IAsyncResult result = receiverContext.Receiver.BeginReceiveBatch(receiverContext.ReceiveBatchSize, receiverContext.ReceiveTimeout, ar =>
                {
                    if (ar.CompletedSynchronously)
                    {
                        return;
                    }

                    var ctx = (ReceiverContext)ar.AsyncState;

                    if (ContinueReceiving(ar, ctx))
                    {
                        ProcessMessages(ctx);
                    }
                },
                receiverContext);

                if (result.CompletedSynchronously)
                {
                    if (ContinueReceiving(result, receiverContext))
                    {
                        goto receive;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // This means the channel is closed
                _trace.TraceError("OperationCanceledException was thrown in trying to receive the message from the service bus.");

                return;
            }
            catch (Exception ex)
            {
                _trace.TraceError(ex.Message);
                receiverContext.OnError(ex);

                Thread.Sleep(RetryDelay);
                goto receive;
                // REVIEW: What should we do here?
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are handled through the error handler callback")]
        private bool ContinueReceiving(IAsyncResult asyncResult, ReceiverContext receiverContext)
        {
            bool shouldContinue = true;
            TimeSpan backoffAmount = BackoffAmount;

            try
            {
                IEnumerable<BrokeredMessage> messages = receiverContext.Receiver.EndReceiveBatch(asyncResult);

                receiverContext.OnMessage(messages);

                // Reset the receive timeout if it changed
                receiverContext.ReceiveTimeout = DefaultReadTimeout;
            }
            catch (ServerBusyException ex)
            {
                receiverContext.OnError(ex);

                // Too busy so back off
                shouldContinue = false;
            }
            catch (OperationCanceledException)
            {
                // This means the channel is closed
                _trace.TraceError("Receiving messages from the service bus threw an OperationCanceledException, most likely due to a closed channel.");

                return false;
            }
            catch (MessagingEntityNotFoundException)
            {
                receiverContext.Receiver.CloseAsync();
                TaskAsyncHelper.Delay(RetryDelay).Then(() => Retry(() => CreateSubscription(receiverContext.ConnectionContext, receiverContext.TopicIndex)));
                return false;
            }
            catch (Exception ex)
            {
                receiverContext.OnError(ex);

                shouldContinue = false;

                // TODO: Exponential backoff
                backoffAmount = ErrorBackOffAmount;

                // After an error, we want to adjust the timeout so that we
                // can recover as quickly as possible even if there's no message
                receiverContext.ReceiveTimeout = ErrorReadTimeout;
            }

            if (!shouldContinue)
            {
                TaskAsyncHelper.Delay(backoffAmount)
                               .Then(ctx => ProcessMessages(ctx), receiverContext);

                return false;
            }

            return true;
        }

        private class ReceiverContext
        {
            public readonly MessageReceiver Receiver;
            public readonly ServiceBusConnectionContext ConnectionContext;

            public int TopicIndex { get; private set; }
            public TimeSpan ReceiveTimeout { get; set; }
            public int ReceiveBatchSize { get; set; }

            public ReceiverContext(int topicIndex,
                                   MessageReceiver receiver,
                                   ServiceBusConnectionContext connectionContext)
            {
                TopicIndex = topicIndex;
                Receiver = receiver;
                ReceiveTimeout = DefaultReadTimeout;
                ReceiveBatchSize = DefaultReceiveBatchSize;
                ConnectionContext = connectionContext;
            }

            public void OnError(Exception ex)
            {
                ConnectionContext.ErrorHandler(TopicIndex, ex);
            }

            public void OnMessage(IEnumerable<BrokeredMessage> messages)
            {
                ConnectionContext.Handler(TopicIndex, messages);
            }
        }


    }
}
