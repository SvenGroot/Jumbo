// $Id$
//
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
        [ConfigurationProperty("readBufferSize", DefaultValue = "64KB", IsRequired = false, IsKey = false)]
        public ByteSize ReadBufferSize
        {
            get { return (ByteSize)this["readBufferSize"]; }
            set { this["readBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the buffer size to use for the <see cref="MergeRecordReader{T}"/>.
        /// </summary>
        [ConfigurationProperty("mergeTaskReadBufferSize", DefaultValue = "1MB", IsRequired = false, IsKey = false)]
        public ByteSize MergeTaskReadBufferSize
        {
            get { return (ByteSize)this["mergeTaskReadBufferSize"]; }
            set { this["mergeTaskReadBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the buffer size used by the file output channel to write to intermediate files.
        /// </summary>
        [ConfigurationProperty("writeBufferSize", DefaultValue = "64KB", IsRequired = false, IsKey = false)]
        public ByteSize WriteBufferSize
        {
            get { return (ByteSize)this["writeBufferSize"]; }
            set { this["writeBufferSize"] = value; }
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
        [ConfigurationProperty("memoryStorageSize", DefaultValue = "100MB", IsRequired = false, IsKey = false)]
        public ByteSize MemoryStorageSize
        {
            get { return (ByteSize)this["memoryStorageSize"]; }
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

        /// <summary>
        /// Gets or sets the number of download threads to use.
        /// </summary>
        [ConfigurationProperty("downloadThreads", DefaultValue = 1, IsRequired = false, IsKey = false)]
        public int DownloadThreads
        {
            get { return (int)this["downloadThreads"]; }
            set { this["downloadThreads"] = value; }
        }
    }
}
