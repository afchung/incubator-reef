using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Attributes;

namespace Org.Apache.REEF.IO.BlockManagement.Block
{
    /// <summary>
    /// Represents the location of the block.
    /// </summary>
    [Unstable("0.14", "New feature. Can change substantially.")]
    public enum Location
    {
        /// <summary>
        /// Block is in memory.
        /// </summary>
        InMemory,

        /// <summary>
        /// Block is stored on disk.
        /// </summary>
        OnDisk,

        /// <summary>
        /// Block is stored on another BlockManager.
        /// </summary>
        Remote,

        /// <summary>
        /// Block has not been ingested yet.
        /// </summary>
        External
    }
}
