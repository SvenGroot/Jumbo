using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Contains constants for use by <see cref="MergeSortTask{T}"/>.
    /// </summary>
    public static class MergeSortTaskConstants
    {
        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specified the maximum number
        /// of files to merge in one pass.
        /// </summary>
        public const string MaxMergeInputsSetting = "MergeSortTask.MaxMergeTasks";
        /// <summary>
        /// The default maximum number of files to merge in one pass.
        /// </summary>
        public const int DefaultMaxMergeInputs = 100;

    }
}
