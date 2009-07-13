using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Provides constants for use with the <see cref="HashPartitioner{T}"/> class.
    /// </summary>
    public static class HashPartitionerConstants
    {
        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specifies the <see cref="IEqualityComparer{T}"/>
        /// to use. If this setting is not specified, <see cref="EqualityComparer{T}.Default"/> will be used.
        /// </summary>
        public const string EqualityComparerSetting = "HashPartitioner.EqualityComparer";
    }
}
