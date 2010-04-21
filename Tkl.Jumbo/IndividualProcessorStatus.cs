// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides usage information for a single processor or core in the system.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The percentages are calculated between two calls of the <see cref="ProcessorStatus.Refresh"/> method, so vary the frequency of the calls to vary the time slice covered by the percentages.
    /// </para>
    /// <para>
    ///   This class is also used to provide combined statistics for all processors via the <see cref="ProcessorStatus"/> property.
    /// </para>
    /// </remarks>
    public sealed class IndividualProcessorStatus
    {
        internal IndividualProcessorStatus(int cpuId)
        {
            CpuId = cpuId;
        }

        /// <summary>
        /// Gets the identifier for this processor, or -1 if this instance represents the combined statistics for all processors.
        /// </summary>
        public int CpuId { get; private set; }
        /// <summary>
        /// Gets the percentage of time the processor was idle.
        /// </summary>
        public float PercentIdleTime { get; internal set; }
        /// <summary>
        /// Gets the percentage of time spent executing user code.
        /// </summary>
        public float PercentUserTime { get; internal set; }
        /// <summary>
        /// Gets the percentage of time spent executing system code.
        /// </summary>
        public float PercentSystemTime { get; internal set; }
        /// <summary>
        /// Gets the percentage of time spent processing hardware and software interrupts.
        /// </summary>
        public float PercentInterruptTime { get; internal set; }
        /// <summary>
        /// Gets the percentage of time spent waiting for I/O.
        /// </summary>
        public float PercentIOWaitTime { get; internal set; }

        /// <summary>
        /// Returns a string representation of the current <see cref="IndividualProcessorStatus"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="IndividualProcessorStatus"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "User: {0:0.0}%; System: {1:0.0}%; Idle: {2:0.0}%; Interrupt: {3:0.0}%; IOWait: {4:0.0}%", PercentUserTime, PercentSystemTime, PercentIdleTime, PercentInterruptTime, PercentIOWaitTime);
        }
    }
}
