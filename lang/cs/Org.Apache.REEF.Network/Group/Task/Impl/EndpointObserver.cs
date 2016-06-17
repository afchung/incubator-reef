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
using System.Reactive.Disposables;
using System.Threading;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal class EndpointObserver : IObserverProxy<NsMessage<GeneralGroupCommunicationMessage>>
    {
        private static readonly Logger Logger = REEF.Utilities.Logging.Logger.GetLogger(typeof(EndpointObserver));

        private readonly ConcurrentDictionary<IObserver<NsMessage<GeneralGroupCommunicationMessage>>, byte> _observers =
            new ConcurrentDictionary<IObserver<NsMessage<GeneralGroupCommunicationMessage>>, byte>();

        private readonly ManualResetEventSlim _subscriptionEvent = new ManualResetEventSlim();

        public IDisposable Subscribe(IObserver<NsMessage<GeneralGroupCommunicationMessage>> observer)
        {
            _observers.TryAdd(observer, new byte());

            return Disposable.Create(() =>
            {
                byte removed;
                _observers.TryRemove(observer, out removed);
            });
        }

        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> value)
        {
            Logger.Log(Level.Error, "GETTING MESSAGE!!!");
            _subscriptionEvent.Wait();
            foreach (var observer in _observers.Keys)
            {
                Logger.Log(Level.Error, "RELAYING!!!");
                observer.OnNext(value);
            }
        }

        public void CompleteSubscription()
        {
            _subscriptionEvent.Set();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}