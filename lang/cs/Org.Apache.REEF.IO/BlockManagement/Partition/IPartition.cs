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
using Org.Apache.REEF.Common.Attributes;
using Org.Apache.REEF.IO.BlockManagement.Block;
using Org.Apache.REEF.IO.BlockManagement.DataSet;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.IO.BlockManagement.Partition
{
    /// <summary>
    /// Composing blocks of a DataSet. A Partition is "owned" by a single BlockManager,
    /// while a BlockManager can "own" many Partitions. In other words, a Partition can only
    /// be appended onto by its "owning" BlockManager. On the other hand a BlockManager can 
    /// read from any Partition it pleases.
    /// </summary>
    [Unstable("0.14", "New feature. Interface can change substantially.")]
    public interface IPartition : IIdentifiable
    {
        /// <summary>
        /// Gets a block within the partition. Does not make
        /// any remote connections.
        /// </summary>
        IBlock GetBlock(string blockId);

        /// <summary>
        /// The DataSet that the Partition is a part of.
        /// </summary>
        IDataSet DataSet { get; }

        /// <summary>
        /// Fetches the Blocks of a partition remotely from the BlockManagerMaster.
        /// </summary>
        IEnumerable<IBlock> FetchBlocks();
    }
}
