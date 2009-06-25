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
        /// Gets or sets the buffer size to use for the <see cref="MergeRecordReader{T}"/>.
        /// </summary>
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

        /// <summary>
        /// Gets or sets the maximum size of the the in-memory input storage.
        /// </summary>
        [ConfigurationProperty("memoryStorageSize", DefaultValue = 0x6400000L, IsRequired = false, IsKey = false)]
        public long MemoryStorageSize
        {
            get { return (long)this["memoryStorageSize"]; }
            set { this["memoryStorageSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the type of compression to use for intermediate files.
        /// </summary>
        [ConfigurationProperty("compressionType", DefaultValue = CompressionType.None, IsRequired = false, IsKey = false)]
        public CompressionType CompressionType
        {
            get { return (CompressionType)this["compressionType"]; }
            set { this["compressionType"] = value; }
        }
    }
}
