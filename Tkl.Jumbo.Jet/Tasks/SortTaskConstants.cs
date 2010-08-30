// $Id$
//
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
        /// to use. The value of the setting is a <see cref="String"/> that specifies the assembly-qualified type name of the comparer.
        /// The default value is <see langword="null"/>, indicating the <see cref="Comparer{T}.Default"/> will be used.
        /// </summary>
        public const string ComparerSettingKey = "SortTask.Comparer";

        /// <summary>
        /// The name of the setting in the <see cref="StageConfiguration.StageSettings"/> or <see cref="JobConfiguration.JobSettings"/> that specifies whether to use
        /// parallel sorting. The type of the setting is <see cref="Boolean"/>. The default value is <see langword="true"/>. Stage settings take precedence over
        /// job settings.
        /// </summary>
        public const string UseParallelSortSettingKey = "SortTask.UseParallelSort";
    }
}
