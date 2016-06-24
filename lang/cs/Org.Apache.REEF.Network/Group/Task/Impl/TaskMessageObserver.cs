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
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class TaskMessageObserver : IObserver<NsMessage<GeneralGroupCommunicationMessage>>
    {
        private readonly Dictionary<NodeObserverIdentifier, IObserver<NsMessage<GeneralGroupCommunicationMessage>>> _observers =
            new Dictionary<NodeObserverIdentifier, IObserver<NsMessage<GeneralGroupCommunicationMessage>>>();

        public void Register<T>(NodeMessageObserver<T> observer)
        {
            _observers.Add(NodeObserverIdentifier.FromObserver(observer), observer);
        }

        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> value)
        {
            var gcMessage = value.Data.First();
            if (string.IsNullOrWhiteSpace(gcMessage.GroupName) 
                || string.IsNullOrWhiteSpace(gcMessage.OperatorName))
            {
                // This is the identifier message.
                return;
            }

            IObserver<NsMessage<GeneralGroupCommunicationMessage>> observer;
            if (!_observers.TryGetValue(NodeObserverIdentifier.FromMessage(gcMessage), out observer))
            {
                throw new InvalidOperationException();
            }

            observer.OnNext(value);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}