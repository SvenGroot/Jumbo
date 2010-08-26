﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration for the <see cref="MergeRecordReader{T}"/>.
    /// </summary>
    public class MergeRecordReaderConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the maxinum number of file inputs to use in a single merge pass.
        /// </summary>
        /// <value>The maximum number of file inputs in a single merge pass. The default value is 100.</value>
        [ConfigurationProperty("maxFileInputs", DefaultValue = 100, IsRequired = false, IsKey = false)]
        [IntegerValidator(MinValue=2)]
        public int MaxFileInputs
        {
            get { return (int)this["maxFileInputs"]; }
            set { this["maxFileInputs"] = value; }
        }

        /// <summary>
        /// Gets or sets the usage level of the channel's memory storage that will trigger a merge pass.
        /// </summary>
        /// <value>The memory storage trigger level, between 0 and 1. The default value is 0.6.</value>
        [ConfigurationProperty("memoryStorageTriggerLevel", DefaultValue = 0.6f, IsRequired = false, IsKey = false)]
        public float MemoryStorageTriggerLevel
        {
            get { return (float)this["memoryStorageTriggerLevel"]; }
            set { this["memoryStorageTriggerLevel"] = value; }
        }

        /// <summary>
        /// Gets or sets the buffer size to use for each input file.
        /// </summary>
        /// <value>The size of the read buffer for each merge stream.</value>
        [ConfigurationProperty("mergeStreamReadBufferSize", DefaultValue = "1MB", IsRequired = false, IsKey = false)]
        public ByteSize MergeStreamReadBufferSize
        {
            get { return (ByteSize)this["mergeStreamReadBufferSize"]; }
            set { this["mergeStreamReadBufferSize"] = value; }
        }
    }
}