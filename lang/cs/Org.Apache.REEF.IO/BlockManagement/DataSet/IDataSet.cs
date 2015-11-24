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
using Org.Apache.REEF.IO.BlockManagement.Partition;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.IO.BlockManagement.DataSet
{
    /// <summary>
    /// The representation of a DataSet in REEF's block management layer.
    /// </summary>
    [Unstable("0.14", "New feature. Interface can change substantially.")]
    public interface IDataSet : IIdentifiable
    {
        /// <summary>
        /// Gets a readonly partition within the DataSet.
        /// </summary>
        IPartition GetPartition(string partitionId);

        /// <summary>
        /// Gets a writable partition within the DataSet.
        /// Fails if the BlockManager cannot append to the partition.
        /// </summary>
        OutputPartition GetOutputPartition(string partitionId);

        /// <summary>
        /// Fetches Partitions from the BlockManagerMaster.
        /// </summary>
        IEnumerable<IPartition> FetchPartitions();
    }
}
