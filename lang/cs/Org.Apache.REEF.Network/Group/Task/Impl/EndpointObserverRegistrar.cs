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

using System.Collections.Generic;
using System.Net;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class EndpointObserverRegistrar
    {
        private readonly object _lock = new object();
        private readonly IDictionary<IPEndPoint, EndpointObserver> _endpointMap =
            new Dictionary<IPEndPoint, EndpointObserver>();
        
        [Inject]
        private EndpointObserverRegistrar()
        {
        }

        public void RegisterEndpointAndObserver<T>(
            IRemoteManager<NsMessage<GeneralGroupCommunicationMessage>> remoteManager, 
            IPEndPoint endpoint,
            NodeMessageObserver<T> nodeMessageObserver)
        {
            lock (_lock)
            {
                if (!_endpointMap.ContainsKey(endpoint))
                {
                    _endpointMap[endpoint] = new EndpointObserver();
                    remoteManager.RegisterObserver(endpoint, _endpointMap[endpoint]);
                }

                _endpointMap[endpoint].Subscribe(nodeMessageObserver);
            }
        }

        public void CompleteSubscription()
        {
            lock (_lock)
            {
                foreach (var endpointObserver in _endpointMap.Values)
                {
                    endpointObserver.CompleteSubscription();
                }
            }
        }
    }
}