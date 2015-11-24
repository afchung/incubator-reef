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
        /// Gets a block within the partition.
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
