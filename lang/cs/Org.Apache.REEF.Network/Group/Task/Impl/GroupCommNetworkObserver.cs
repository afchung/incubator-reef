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
using System.Linq;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// Writable version
    /// </summary>
    internal sealed class GroupCommNetworkObserver : IObserver<NsMessage<GeneralGroupCommunicationMessage>>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(GroupCommNetworkObserver));

        private readonly CommunicationGroupContainer _commGroupContainer;

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        internal GroupCommNetworkObserver(CommunicationGroupContainer container)
        {
            _commGroupContainer = container;
        }

        /// <summary>
        /// Handles the incoming WritableNsMessage for this Task.
        /// Delegates the GeneralGroupCommunicationMessage to the correct 
        /// WritableCommunicationGroupNetworkObserver.
        /// </summary>
        /// <param name="nsMessage"></param>
        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> nsMessage)
        {
            if (nsMessage == null)
            {
                throw new ArgumentNullException("nsMessage");
            }

            try
            {
                GeneralGroupCommunicationMessage gcm = nsMessage.Data.First();
                IObserver<GeneralGroupCommunicationMessage> handler;
                if (!_commGroupContainer.GetCommunicationGroupHandlerByName(gcm.GroupName, gcm.OperatorName, gcm.Source, out handler))
                {
                    throw new ApplicationException("Group Communication Network Handler received message for nonexistant group");
                }
                
                handler.OnNext(gcm);
            }
            catch (InvalidOperationException ioe)
            {
                const string message = "Group Communication Network Handler received message with no data";
                LOGGER.Log(Level.Error, message);
                throw new ApplicationException(message, ioe);
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
