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
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class NodeMessageObserver<T> : IObserver<NsMessage<GeneralGroupCommunicationMessage>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(NodeMessageObserver<>));
        private readonly NodeStruct<T> _nodeStruct;

        internal NodeMessageObserver(NodeStruct<T> nodeStruct)
        {
            _nodeStruct = nodeStruct;
        }

        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> value)
        {
            foreach (var data in value.Data)
            {
                var gcMessage = data as GroupCommunicationMessage<T>;
                if (gcMessage != null)
                {
                    _nodeStruct.AddData(gcMessage);
                }
            }
        }

        public string SourceId
        {
            get { return _nodeStruct.Identifier; }
        }

        public string GroupId
        {
            get { return _nodeStruct.GroupId; }
        }

        public string OperatorName
        {
            get { return _nodeStruct.OperatorName; }
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