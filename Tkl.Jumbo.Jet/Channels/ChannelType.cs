using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Channels
{
    /// <summary>
    /// The type of a communication channel between two tasks.
    /// </summary>
    public enum ChannelType
    {
        /// <summary>
        /// The input task writes a file to disk, which the output task then downloads and reads from.
        /// </summary>
        File
    }
}
