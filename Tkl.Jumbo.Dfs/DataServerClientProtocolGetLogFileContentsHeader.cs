using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Dfs
{
    /// <summary>
    /// Header sent to the data server for the GetLogFileContents command.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolGetLogFileContentsHeader : DataServerClientProtocolHeader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolGetLogFileContentsHeader"/> class with
        /// the specified maximum size.
        /// </summary>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        public DataServerClientProtocolGetLogFileContentsHeader(int maxSize)
            : base(DataServerCommand.GetLogFileContents)
        {
            MaxSize = maxSize;
        }

        /// <summary>
        /// Gets the maximum size of the log data to return.
        /// </summary>
        public int MaxSize { get; private set; }
    }
}
