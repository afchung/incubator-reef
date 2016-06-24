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

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    internal sealed class NodeObserverIdentifier
    {
        private readonly Type _type;
        private readonly string _groupName;
        private readonly string _operatorName;

        public static NodeObserverIdentifier FromNode<T>(NodeStruct<T> node)
        {
            return new NodeObserverIdentifier(typeof(T), node.GroupName, node.OperatorName);
        }

        public static NodeObserverIdentifier FromObserver<T>(NodeMessageObserver<T> observer)
        {
            return new NodeObserverIdentifier(typeof(T), observer.GroupName, observer.OperatorName);
        }

        public static NodeObserverIdentifier FromMessage(GeneralGroupCommunicationMessage message)
        {
            return new NodeObserverIdentifier(message.Type, message.GroupName, message.OperatorName);
        }

        private NodeObserverIdentifier(Type type, string groupName, string operatorName)
        {
            _type = type;
            _groupName = groupName;
            _operatorName = operatorName;
        }

        public Type Type
        {
            get { return _type; }
        }

        public string GroupName
        {
            get { return _groupName; }
        }

        public string OperatorName
        {
            get { return _operatorName; }
        }

        private bool Equals(NodeObserverIdentifier other)
        {
            return _type == other.Type &&
                _groupName.Equals(other.GroupName) &&
                _operatorName.Equals(other.OperatorName);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj is NodeObserverIdentifier && Equals((NodeObserverIdentifier)obj);
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = (hash * 31) + _type.GetHashCode();
            hash = (hash * 31) + _groupName.GetHashCode();
            return (hash * 31) + _operatorName.GetHashCode();
        }
    }
}