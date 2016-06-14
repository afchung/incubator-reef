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
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class CommunicationGroupContainer
    {
        private readonly IDictionary<Tuple<string, string, string>, IObserver<GeneralGroupCommunicationMessage>> _groupDictionary = 
            new Dictionary<Tuple<string, string, string>, IObserver<GeneralGroupCommunicationMessage>>();
        
        [Inject]
        private CommunicationGroupContainer()
        {
        }

        public bool GetCommunicationGroupHandlerByName(
            string groupName, string operatorName, string source, out IObserver<GeneralGroupCommunicationMessage> handler)
        {
            return _groupDictionary.TryGetValue(new Tuple<string, string, string>(groupName, operatorName, source), out handler);
        }

        public void RegisterCommunicationGroupHandler(
            string groupName, string operatorName, string source, IObserver<GeneralGroupCommunicationMessage> handler)
        {
            _groupDictionary[new Tuple<string, string, string>(groupName, operatorName, source)] = handler;
        }
    }
}