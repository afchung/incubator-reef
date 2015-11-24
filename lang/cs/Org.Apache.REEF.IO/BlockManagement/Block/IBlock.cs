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
    public interface IBlock : IIdentifiable
    {
        /// <summary>
        /// The partition that the block belongs to.
        /// </summary>
        IPartition Partition { get; }

        /// <summary>
        /// The lowest level where the block is managed by the current BlockManager.
        /// </summary>
        Location Location { get; }

        /// <summary>
        /// Opens the block.
        /// </summary>
        Stream DataStream { get; }

        /// <summary>
        /// Localizes a block and caches in memory if not already localized.
        /// The BlockManager will register the newly localized block
        /// with the BlockManagerMaster.
        /// </summary>
        void LocalizeBlockInMemory();

        /// <summary>
        /// Localizes a block and puts onto disk if not already on disk.
        /// The BlockManager will register the newly localized block with
        /// the BlockManagerMaster.
        /// </summary>
        void LocalizeBlockOnDisk(string filePath);
    }
}
