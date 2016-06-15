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
using System.Collections.Generic;
using System.Net;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Stores registered IObservers for DefaultRemoteManager.
    /// Can register and look up IObservers by remote IPEndPoint.
    /// </summary>
    internal sealed class ObserverContainer<T> : IObserver<TransportEvent<IRemoteEvent<T>>>
    {
        private readonly ConcurrentDictionary<IPEndPoint, ConcurrentDictionary<IObserver<T>, byte>> _endpointMap;
        private readonly ISet<IObserver<IRemoteMessage<T>>> _universalObservers = new HashSet<IObserver<IRemoteMessage<T>>>();

        /// <summary>
        /// Constructs a new ObserverContainer used to manage remote IObservers.
        /// </summary>
        public ObserverContainer()
        {
            _endpointMap = new ConcurrentDictionary<IPEndPoint, ConcurrentDictionary<IObserver<T>, byte>>(new IPEndPointComparer());
        }

        /// <summary>
        /// Registers an IObserver used to handle incoming messages from the remote host
        /// at the specified IPEndPoint.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndPoint of the remote host</param>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(IPEndPoint remoteEndpoint, IObserver<T> observer) 
        {
            if (remoteEndpoint.Address.Equals(IPAddress.Any))
            {
                var universalObserver = new UniversalObserverWrapper(observer);
                _universalObservers.Add(universalObserver);
                return Disposable.Create(() => _universalObservers.Remove(universalObserver));
            }

            if (!_endpointMap.ContainsKey(remoteEndpoint))
            {
                _endpointMap.TryAdd(remoteEndpoint, new ConcurrentDictionary<IObserver<T>, byte>());
            }

            _endpointMap[remoteEndpoint].TryAdd(observer, new byte());
            return Disposable.Create(() =>
            {
                ConcurrentDictionary<IObserver<T>, byte> set;
                if (_endpointMap.TryGetValue(remoteEndpoint, out set))
                {
                    byte outByte;
                    set.TryRemove(observer, out outByte);
                }
            });
        }

        /// <summary>
        /// Registers an IObserver to handle incoming messages from a remote host
        /// </summary>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterUniversalObserver(IObserver<IRemoteMessage<T>> observer)
        {
            _universalObservers.Add(observer);
            
            return Disposable.Create(() =>
            {
                _universalObservers.Remove(observer);
            });
        }

        /// <summary>
        /// Look up the IObserver for the registered IPEndPoint or event type 
        /// and execute the IObserver.
        /// </summary>
        /// <param name="transportEvent">The incoming remote event</param>
        public void OnNext(TransportEvent<IRemoteEvent<T>> transportEvent)
        {
            IRemoteEvent<T> remoteEvent = transportEvent.Data;
            T value = remoteEvent.Value;
            bool handled = false;

            // Universal observers should always be invoked prior to individual
            // observers, as universal observers can register individual observers, as in the case of
            // NetworkObserverFactoryObserver.
            foreach (var universalObserver in _universalObservers)
            {
                // IObserver was registered by event type
                Logger.GetLogger("HELLOLOGGER").Log(Level.Error, "AAAAA " + remoteEvent.RemoteEndPoint);
                IRemoteIdentifier id = new SocketRemoteIdentifier(remoteEvent.RemoteEndPoint);
                IRemoteMessage<T> remoteMessage = new DefaultRemoteMessage<T>(id, value);
                universalObserver.OnNext(remoteMessage);
                handled = true;
            }

            ConcurrentDictionary<IObserver<T>, byte> observerSet;
            if (_endpointMap.TryGetValue(remoteEvent.RemoteEndPoint, out observerSet))
            {
                foreach (var observer in observerSet.Keys)
                {
                    // IObserver was registered by IPEndpoint
                    observer.OnNext(value);
                    handled = true;
                }
            }

            if (!handled)
            {
                throw new WakeRuntimeException("Unrecognized Wake RemoteEvent message");
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private sealed class UniversalObserverWrapper : IObserver<IRemoteMessage<T>>
        {
            private readonly IObserver<T> _observer;

            internal UniversalObserverWrapper(IObserver<T> observer)
            {
                _observer = observer;
            }

            public void OnNext(IRemoteMessage<T> value)
            {
                _observer.OnNext(value.Message);
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }
    }
}
