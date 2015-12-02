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
using Org.Apache.REEF.Common.Attributes;
using Org.Apache.REEF.IO.BlockManagement.Partition;

namespace Org.Apache.REEF.IO.BlockManagement.Block
{
    /// <summary>
    /// A readonly implementation of <see cref="IBlock"/>.
    /// </summary>
    [Unstable("0.14", "New feature. Implementation can change substantially.")]
    public sealed class ReadonlyBlock : IBlock
    {
        public Guid Uid { get; private set; }
        
        public IPartition Partition { get; private set; }

        public Location Location { get; private set; }

        public bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public IList<byte> Data
        {
            get
            {
                // TODO: Lazily fetch data into memory for consumption.
                // TODO: Register with BlockManager that there is local copy.
                // TODO: Return IReadOnlyList.
                Location = Location.InMemory;
                throw new NotImplementedException();
            }

            set
            {
                throw new NotSupportedException("Readable blocks are not set-able.");
            }
        }
    }
}
