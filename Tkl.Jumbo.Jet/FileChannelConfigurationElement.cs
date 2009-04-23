using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for the file channel.
    /// </summary>
    public class FileChannelConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the buffer size to use for input to push and pull tasks.
        /// </summary>
        [ConfigurationProperty("readBufferSize", DefaultValue = 65536, IsRequired = false, IsKey = false)]
        public int ReadBufferSize
        {
            get { return (int)this["readBufferSize"]; }
            set { this["readBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the default buffer size to use for merge input tasks.
        /// </summary>
        /// <remarks>
        /// Because merge tasks tend to read from several files at once, this buffer should usually be quite large to reduce seek overhead (the default value
        /// is 1MB). Merge tasks can override this value by setting <see cref="MergeTaskInput{T}.BufferSize"/>.
        /// </remarks>
        [ConfigurationProperty("mergeTaskReadBufferSize", DefaultValue = 0x100000, IsRequired = false, IsKey = false)]
        public int MergeTaskReadBufferSize
        {
            get { return (int)this["mergeTaskReadBufferSize"]; }
            set { this["mergeTaskReadBufferSize"] = value; }
        }
        /// <summary>
        /// Gets or sets a value that indicates whether intermediate files should be deleted after the task or job is finished.
        /// </summary>
        [ConfigurationProperty("deleteIntermediateFiles", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool DeleteIntermediateFiles
        {
            get { return (bool)this["deleteIntermediateFiles"]; }
            set { this["deleteIntermediateFiles"] = value; }
        }
    }
}
