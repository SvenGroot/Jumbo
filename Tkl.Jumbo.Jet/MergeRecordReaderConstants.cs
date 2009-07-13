using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Contains constants for use by <see cref="MergeRecordReader{T}"/>.
    /// </summary>
    public static class MergeRecordReaderConstants
    {
        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specifies the maximum number
        /// of files to merge in one pass.
        /// </summary>
        public const string MaxMergeInputsSetting = "MergeRecordReader.MaxMergeTasks";

        /// <summary>
        /// The default maximum number of files to merge in one pass.
        /// </summary>
        public const int DefaultMaxMergeInputs = 100;

        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specifies the <see cref="IComparer{T}"/>
        /// to use. If this setting is not specified, <see cref="Comparer{T}.Default"/> will be used.
        /// </summary>
        public const string ComparerSetting = "MergeRecordReader.Comparer";
    }
}
