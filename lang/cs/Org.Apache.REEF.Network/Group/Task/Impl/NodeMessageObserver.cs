// Licensed to the Apache Software Foundation (ASF) under one
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
using Org.Apache.REEF.Network.Group.Driver.Impl;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class NodeMessageObserver<T> : IObserver<GroupCommunicationMessage<T>>
    {
        private readonly BlockingCollection<NodeStruct<T>> _queue;
        private readonly NodeStruct<T> _nodeStruct;

        internal NodeMessageObserver(BlockingCollection<NodeStruct<T>> queue, NodeStruct<T> nodeStruct)
        {
            _queue = queue;
            _nodeStruct = nodeStruct;
        }

        public void OnNext(GroupCommunicationMessage<T> value)
        {
            _nodeStruct.AddData(value);
            _queue.Add(_nodeStruct);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        internal string GroupName
        {
            get { return _nodeStruct.GroupName; }
        }
    }
}