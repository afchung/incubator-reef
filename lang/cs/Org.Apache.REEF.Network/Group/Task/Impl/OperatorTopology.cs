﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Contains the Operator's topology graph.
    /// Writable version
    /// Used to send or receive messages to/from operators in the same
    /// Communication Group.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public sealed class OperatorTopology<T> : IOperatorTopology<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopology<>));

        private readonly string _groupName;
        private readonly string _operatorName;
        private readonly string _selfId;
        private readonly int _retryCount;
        private readonly int _sleepTime;

        private readonly long _timeout;
        private readonly NodeStruct<T> _parent;
        private readonly List<NodeStruct<T>> _children;
        private readonly Dictionary<string, NodeStruct<T>> _idToNodeMap;
        private readonly Sender _sender;
        private readonly IRemoteManager<NsMessage<GeneralGroupCommunicationMessage>> _remoteManager;
        private readonly EndpointObserverRegistrar _registrar;
        private readonly INameClient _nameClient;

        /// <summary>
        /// Creates a new OperatorTopology object.
        /// </summary>
        /// <param name="operatorName">The name of the Group Communication Operator</param>
        /// <param name="groupName">The name of the operator's Communication Group</param>
        /// <param name="taskId">The operator's Task identifier</param>
        /// <param name="timeout">Timeout value for cancellation token</param>
        /// <param name="retryCount">Number of times to retry wating for registration</param>
        /// <param name="sleepTime">Sleep time between retry wating for registration</param>
        /// <param name="rootId">The identifier for the root Task in the topology graph</param>
        /// <param name="childIds">The set of child Task identifiers in the topology graph</param>
        /// <param name="sender">The Sender used to do point to point communication</param>
        /// <param name="networkService"></param>
        /// <param name="registrar"></param>
        [Inject]
        private OperatorTopology(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommConfigurationOptions.RetryCountWaitingForRegistration))] int retryCount,
            [Parameter(typeof(GroupCommConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyRootTaskId))] string rootId,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyChildTaskIds))] ISet<string> childIds,
            Sender sender,
            StreamingNetworkService<GeneralGroupCommunicationMessage> networkService,
            EndpointObserverRegistrar registrar)
        {
            _operatorName = operatorName;
            _groupName = groupName;
            _selfId = taskId;
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _sender = sender;
            _children = new List<NodeStruct<T>>();
            _idToNodeMap = new Dictionary<string, NodeStruct<T>>();
            _nameClient = networkService.NamingClient;
            _remoteManager = networkService.RemoteManager;
            _registrar = registrar;

            if (_selfId.Equals(rootId))
            {
                _parent = null;
            }
            else
            {
                _parent = new NodeStruct<T>(rootId, groupName);
                _idToNodeMap[rootId] = _parent;
            }
            foreach (var childId in childIds)
            {
                var node = new NodeStruct<T>(childId, groupName);
                _children.Add(node);
                _idToNodeMap[childId] = node;
            }
        }

        /// <summary>
        /// Initializes operator topology.
        /// Waits until all Tasks in the CommunicationGroup have registered themselves
        /// with the Name Service.
        /// </summary>
        public void Initialize()
        {
            using (Logger.LogFunction("OperatorTopology::Initialize"))
            {
                if (_parent != null)
                {
                    WaitForTaskRegistration(_parent.Identifier, _retryCount, new NodeMessageObserver<T>(_parent));
                }

                if (_children.Count > 0)
                {
                    foreach (var child in _children)
                    {
                        WaitForTaskRegistration(
                            child.Identifier, _retryCount, new NodeMessageObserver<T>(child));
                    }
                }
            }
        }

        /// <summary>
        /// Sends the message to the parent Task.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        public void SendToParent(T message, MessageType type)
        {
            if (_parent == null)
            {
                throw new ArgumentException("No parent for node");
            }

            SendToNode(message, _parent);
        }

        /// <summary>
        /// Sends the message to all child Tasks.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        public void SendToChildren(T message, MessageType type)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            foreach (var child in _children)
            {
                SendToNode(message, child);
            }
        }

        /// <summary>
        /// Splits the list of messages up evenly and sends each sublist
        /// to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(IList<T> messages, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (_children.Count <= 0)
            {
                return;
            }

            var count = (int)Math.Ceiling(((double)messages.Count) / _children.Count);
            ScatterHelper(messages, _children, count);
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="count">The size of each sublist</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(IList<T> messages, int count, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (count <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            ScatterHelper(messages, _children, count);
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks in the specified order.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="order">The order to send messages</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(IList<T> messages, List<string> order, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (order == null || order.Count != _children.Count)
            {
                throw new ArgumentException("order cannot be null and must have the same number of elements as child tasks");
            }

            List<NodeStruct<T>> nodes = new List<NodeStruct<T>>();
            foreach (string taskId in order)
            {
                NodeStruct<T> node = FindNode(taskId);
                if (node == null)
                {
                    throw new IllegalStateException("Received message from invalid task id: " + taskId);
                }

                nodes.Add(node);
            }

            int count = (int)Math.Ceiling(((double)messages.Count) / _children.Count);
            ScatterHelper(messages, nodes, count);
        }

        /// <summary>
        /// Receive an incoming message from the parent Task.
        /// </summary>
        /// <returns>The parent Task's message</returns>
        public T ReceiveFromParent()
        {
            T[] data = ReceiveFromNode(_parent);
            if (data == null || data.Length != 1)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data[0];
        }

        public IList<T> ReceiveListFromParent()
        {
            T[] data = ReceiveFromNode(_parent);
            if (data == null || data.Length == 0)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data.ToList();
        }

        /// <summary>
        /// Receives all messages from child Tasks and reduces them with the
        /// given IReduceFunction.
        /// </summary>
        /// <param name="reduceFunction">The class used to reduce messages</param>
        /// <returns>The result of reducing messages</returns>
        public T ReceiveFromChildren(IReduceFunction<T> reduceFunction)
        {
            if (reduceFunction == null)
            {
                throw new ArgumentNullException("reduceFunction");
            }

            var receivedData = new ConcurrentQueue<T>();

            Parallel.ForEach(_children,
                child =>
                {
                    receivedData.Enqueue(child.GetData()[0]);
                });

            return reduceFunction.Reduce(receivedData);
        }

        public bool HasChildren()
        {
            return _children.Count > 0;
        }

        /// <summary>
        /// Sends the message to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(T message, NodeStruct<T> node)
        {
            GeneralGroupCommunicationMessage gcm = new GroupCommunicationMessage<T>(_groupName, _operatorName,
                _selfId, node.Identifier, message);

            Logger.Log(Level.Info, "SENDTONODE ID: " + node.Identifier + " MY ID: " + _selfId);
            _sender.Send(gcm);
        }

        /// <summary>
        /// Sends the list of messages to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="messages">The list of messages to send</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(IList<T> messages, NodeStruct<T> node)
        {
            T[] encodedMessages = messages.ToArray();

            GroupCommunicationMessage<T> gcm = new GroupCommunicationMessage<T>(_groupName, _operatorName,
                _selfId, node.Identifier, encodedMessages);

            Logger.Log(Level.Info, "SENDTONODE ID: " + node.Identifier + " MY ID: " + _selfId);
            _sender.Send(gcm);
            Logger.Log(Level.Info, "SENTTONODE ID: " + node.Identifier + " MY ID: " + _selfId);
        }

        /// <summary>
        /// Receive a message from the Task represented by the given NodeStruct.
        /// Removes the NodeStruct from the nodesWithData queue if requested.
        /// </summary>
        /// <param name="node">The node to receive from</param>
        /// <returns>The byte array message from the node</returns>
        private T[] ReceiveFromNode(NodeStruct<T> node)
        {
            var data = node.GetData();
            return data;
        }

        /// <summary>
        /// Find the NodeStruct with the given Task identifier.
        /// </summary>
        /// <param name="identifier">The identifier of the Task</param>
        /// <returns>The NodeStruct</returns>
        private NodeStruct<T> FindNode(string identifier)
        {
            NodeStruct<T> node;
            return _idToNodeMap.TryGetValue(identifier, out node) ? node : null;
        }

        private void ScatterHelper(IList<T> messages, List<NodeStruct<T>> order, int count)
        {
            Logger.Log(Level.Info, "SCATTERING TO " + order.Count + " CHILDREN!!!");
            if (count <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            int i = 0;
            foreach (var nodeStruct in order)
            {
                // The last sublist might be smaller than count if the number of
                // child tasks is not evenly divisible by count
                int left = messages.Count - i;
                int size = (left < count) ? left : count;
                if (size <= 0)
                {
                    throw new ArgumentException("Scatter count must be positive");
                }

                IList<T> sublist = messages.ToList().GetRange(i, size);
                SendToNode(sublist, nodeStruct);

                i += size;
            }
        }

        /// <summary>
        /// Checks if the identifier is registered with the Name Server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifier">The identifier to look up</param>
        /// <param name="retries">The number of times to retry the lookup operation</param>
        /// <param name="nodeMessageObserver"></param>
        private void WaitForTaskRegistration(string identifier, int retries, NodeMessageObserver<T> nodeMessageObserver)
        {
            for (int i = 0; i < retries; i++)
            {
                var ipEndpoint = _nameClient.Lookup(identifier);
                if (ipEndpoint != null)
                {
                    _registrar.RegisterEndpointAndObserver(_remoteManager, ipEndpoint, nodeMessageObserver);
                    return;
                }

                Thread.Sleep(_sleepTime);
            }

            throw new IllegalStateException("Failed to initialize operator topology for node: " + identifier);
        }
    }
}
