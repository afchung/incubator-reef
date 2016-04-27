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
using System.IO;

namespace Org.Apache.REEF.IO.DataCache.DataRepresentations
{
    /// <summary>
    /// A serialized representation of the data, stored in a <see cref="List{MemoryStream}"/>.
    /// </summary>
    public sealed class MemoryStreamsDataRepresentation<T> : IDataRepresentation<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly IList<MemoryStream> _streams;
        private readonly bool _leaveOpen;

        public MemoryStreamsDataRepresentation(ISerializer<T> serializer, IEnumerable<MemoryStream> streams, bool leaveOpen = false)
        {
            _serializer = serializer;
            _streams = new List<MemoryStream>(streams);
            _leaveOpen = leaveOpen;
        } 

        public T ToData()
        {
            return _serializer.Deserialize(_streams);
        }

        public void Dispose()
        {
            if (!_leaveOpen)
            {
                foreach (var stream in _streams)
                {
                    stream.Dispose();
                }
            }

            _streams.Clear();
        }
    }
}