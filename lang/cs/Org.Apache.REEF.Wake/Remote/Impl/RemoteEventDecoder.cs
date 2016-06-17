﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.Net;
using Org.Apache.REEF.Wake.Remote.Proto;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    public class RemoteEventDecoder<T> : IDecoder<IRemoteEvent<T>>
    {
        private readonly IDecoder<T> _decoder;

        public RemoteEventDecoder(IDecoder<T> decoder)
        {
            _decoder = decoder;
        }

        public IRemoteEvent<T> Decode(byte[] data)
        {
            WakeMessagePBuf pbuf = WakeMessagePBuf.Deserialize(data);
            return new RemoteEvent<T>(
                IPEndpointFromString(pbuf.sink), IPEndpointFromString(pbuf.source), pbuf.seq, _decoder.Decode(pbuf.data));
        }

        private static IPEndPoint IPEndpointFromString(string ipEndpointStr)
        {
            var pair = ipEndpointStr.Split(':');
            if (pair.Length != 2)
            {
                throw new ArgumentException("Expected IPEndpoint string to be of format host:port.");
            }

            try
            {
                return new IPEndPoint(IPAddress.Parse(pair[0]), int.Parse(pair[1]));
            }
            catch (Exception e)
            {
                throw new ArgumentException("IPEndpoint string must be of format host:port, where port is an integer.", e);
            }
        }
    }
}
