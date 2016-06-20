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

using System.Net;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    internal sealed class RemoteEvent<T> : IRemoteEvent<T>
    {
        /// <summary>
        /// Constructor for the first RemoteEvent on the sending side.
        /// </summary>
        public RemoteEvent(IPEndPoint localEndpoint, IPEndPoint remoteEndpoint, T value)
            : this(localEndpoint, remoteEndpoint, 0, value)
        {
        }

        /// <summary>
        /// Constructor for subsequent RemoteEvents on the sending side.
        /// </summary>
        public RemoteEvent(long seq, T value)
            : this(null, null, seq, value)
        {
        }

        /// <summary>
        /// Constructor for Decoder.
        /// </summary>
        public RemoteEvent(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, long seq, T value)
        {
            LocalEndPoint = localEndPoint;
            RemoteEndPoint = remoteEndPoint;
            Value = value;
            Sequence = seq;
        }

        public IPEndPoint LocalEndPoint { get; private set; }

        public IPEndPoint RemoteEndPoint { get; private set; }

        public T Value { get; private set; }

        public long Sequence { get; private set; }
    }
}
