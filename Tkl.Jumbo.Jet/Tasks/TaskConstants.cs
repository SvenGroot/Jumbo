﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Provides constants for use with the built-in tasks.
    /// </summary>
    public static class TaskConstants
    {
        /// <summary>
        /// The name of the setting in <see cref="StageConfiguration.StageSettings"/> that specifies the <see cref="IComparer{T}"/>
        /// to use for the <see cref="SortTask{T}"/>. The value of the setting is a <see cref="String"/> that specifies the assembly-qualified type name of the comparer.
        /// The default value is <see langword="null"/>, indicating the <see cref="Comparer{T}.Default"/> will be used.
        /// </summary>
        public const string ComparerSettingKey = "SortTask.Comparer";

        /// <summary>
        /// The name of the setting in the <see cref="StageConfiguration.StageSettings"/> or <see cref="JobConfiguration.JobSettings"/> that specifies whether to use
        /// parallel sorting in the <see cref="SortTask{T}"/>. The type of the setting is <see cref="Boolean"/>. The default value is <see langword="true"/>. Stage settings take precedence over
        /// job settings.
        /// </summary>
        public const string UseParallelSortSettingKey = "SortTask.UseParallelSort";

        /// <summary>
        /// The name of the setting in the <see cref="StageConfiguration.StageSettings"/> that determines the default value assigned to every key/value pair by
        /// the <see cref="GenerateInt32PairTask{TKey}"/>. The type of the setting is <see cref="Int32"/>. This setting can only be
        /// specified in the stage settings, not in the job settings.
        /// </summary>
        public const string GeneratePairTaskDefaultValueKey = "GeneratePairTask.DefaultValue";
    }
}