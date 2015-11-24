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
        /// Fetches Partitions from the BlockManagerMaster.
        /// </summary>
        IEnumerable<IPartition> FetchPartitions();
    }
}
