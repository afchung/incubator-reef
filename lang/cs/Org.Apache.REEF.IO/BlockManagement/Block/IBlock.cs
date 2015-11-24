using System.IO;
using Org.Apache.REEF.Common.Attributes;
using Org.Apache.REEF.IO.BlockManagement.Partition;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.IO.BlockManagement.Block
{
    [Unstable("0.14", "New feature. Interface can change substantially.")]
    public interface IBlock : IIdentifiable
    {
        IPartition Partition { get; }

        Stream DataStream { get; }

        /// <summary>
        /// Localizes a block if not already localized.
        /// The BlockManagerMaster will register the the newly 
        /// localized block with the corresponding local BlockManager.
        /// </summary>
        IBlock LocalizeBlockInMemory();
    }
}
