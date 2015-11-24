using System;
using System.Collections.Generic;
using Org.Apache.REEF.IO.BlockManagement.Block;
using Org.Apache.REEF.IO.BlockManagement.DataSet;

namespace Org.Apache.REEF.IO.BlockManagement.Partition
{
    /// <summary>
    /// An appendable partition.
    /// </summary>
    public sealed class OutputPartition : IPartition
    {
        private OutputPartition()
        {
            throw new NotImplementedException();
        }

        public string Id { get; private set; }

        public IBlock GetBlock(string blockId)
        {
            throw new NotImplementedException();
        }

        public IDataSet DataSet { get; private set; }

        public IEnumerable<IBlock> FetchBlocks()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Appends a block to the OutputPartition.
        /// </summary>
        public void AppendBlock(IBlock block)
        {
            throw new NotImplementedException();
        }
    }
}
