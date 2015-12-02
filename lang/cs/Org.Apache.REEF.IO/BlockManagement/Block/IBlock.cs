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
using Org.Apache.REEF.Common.Attributes;
using Org.Apache.REEF.IO.BlockManagement.Partition;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.IO.BlockManagement.Block
{
    /// <summary>
    /// The atomic unit of the Block Management framework in REEF.
    /// </summary>
    [Unstable("0.14", "New feature. Interface can change substantially.")]
    internal interface IBlock : IUniquelyIdentifiable
    {
        /// <summary>
        /// The partition that the block belongs to.
        /// Can potentially make a remote connection if the block was retrieved only 
        /// with the Uid.
        /// </summary>
        IPartition Partition { get; }

        /// <summary>
        /// The lowest level where the block is managed by the current BlockManager.
        /// </summary>
        Location Location { get; }

        /// <summary>
        /// Whether the block can be written to.
        /// </summary>
        bool CanWrite { get; }

        /// <summary>
        /// Localizes and "opens" the block.
        /// </summary>
        IList<byte> Data { get; set; }
    }
}
