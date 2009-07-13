using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Provides constants for use with the <see cref="SortTask{T}"/> class.
    /// </summary>
    public static class SortTaskConstants
    {
        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specifies the <see cref="IComparer{T}"/>
        /// to use. If this setting is not specified, <see cref="Comparer{T}.Default"/> will be used.
        /// </summary>
        public const string ComparerSetting = "SortTask.Comparer";
    }
}
