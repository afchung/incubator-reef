using System;
using System.Collections.Generic;
using Org.Apache.REEF.IO.BlockManagement.Block;
using Org.Apache.REEF.IO.BlockManagement.DataSet;

namespace Org.Apache.REEF.IO.BlockManagement.Partition
{
    public sealed class InputPartition : IPartition
    {
        private InputPartition()
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
    }
}
