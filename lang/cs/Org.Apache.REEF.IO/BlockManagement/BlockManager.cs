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
using Org.Apache.REEF.IO.BlockManagement.Block;
using Org.Apache.REEF.IO.BlockManagement.DataSet;
using Org.Apache.REEF.IO.BlockManagement.Parameters;
using Org.Apache.REEF.IO.BlockManagement.Partition;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.BlockManagement
{
    [Unstable("0.14", "New feature. Implementation can change substantially.")]
    public sealed class BlockManager
    {
        private readonly ISet<string> _partitionIds;
            
        [Inject]
        private BlockManager([Parameter(typeof(PartitionIds))] ISet<string> partitionIds)
        {
            _partitionIds = partitionIds;
        }

        /// <summary>
        /// Gets the partition IDs that the block manager is able to write to.
        /// </summary>
        public ISet<string> PartitionIds
        {
            get
            {
                return new HashSet<string>(_partitionIds);
            }
        }

        /// <summary>
        /// Entry point for all operations on the block management layer.
        /// Gets a DataSet. Does not make any remote connections. The DataSet
        /// will only be verified when a data read or write operation occurs on
        /// one if its composing blocks, its partitions are fetched, its blocks
        /// are fetched, or when a block is created on the DataSet.
        /// </summary>
        public IDataSet GetDataSet(string dataSetId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by <see cref="IDataSet.GetPartition"/> to return a
        /// Partition of the DataSet. Does not make remote requests. The Partition
        /// will only be verified when a data read or write operation occurs on 
        /// one of its composing blocks, or if blocks are fetched.
        /// </summary>
        internal IPartition GetPartition(IDataSet dataSet, string partitionId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by <see cref="IDataSet.FetchPartitions"/> to internally fetch
        /// partitions from the BlockManagerMaster.
        /// </summary>
        internal IEnumerable<IPartition> FetchPartitions(IDataSet dataSet)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by <see cref="IPartition.FetchBlocks"/> to internally fetch
        /// blocks from the BlockManagerMaster.
        /// </summary>
        internal IEnumerable<IBlock> FetchBlocks(IPartition partition)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by <see cref="IBlock.LocalizeBlockInMemory"/>. Contacts the
        /// BlockManagerMaster if Block is not already local to learn where the
        /// Block is. Saves the block in memory.
        /// If the Block is on another BlockManager, the BlockManager will contact
        /// the other BlockManager to fetch the data.
        /// If the Block is on an external source, the BlockManager will fetch the Block
        /// based on its description.
        /// </summary>
        internal void LocalizeBlockInMemory()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called by <see cref="IBlock.LocalizeBlockOnDisk"/>. Contacts the
        /// BlockManagerMaster if Block is not already local to learn where the
        /// Block is. Saves the block on disk.
        /// If the Block is on another BlockManager, the BlockManager will contact
        /// the other BlockManager to fetch the data.
        /// If the Block is on an external source, the BlockManager will fetch the Block
        /// based on its description.
        /// </summary>
        internal void LocalizeBlockOnDisk(string filePath)
        {
            throw new NotImplementedException();
        }
    }
}
