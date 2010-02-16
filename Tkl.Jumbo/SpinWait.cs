using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides spin-based waiting functionality.
    /// </summary>
    /// <remarks>
    /// The public API is based on .Net 4.0's SpinWait class so once Mono supports that we can switch if desired.
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1815:OverrideEqualsAndOperatorEqualsOnValueTypes")]
    public struct SpinWait
    {
        private const int _yieldInterval = 0x19;
        private static readonly bool _isSingleCpu = Environment.ProcessorCount == 1; // Unlikely to change during execution. :)
        private int _count;

        /// <summary>
        /// Gets the number of times <see cref="SpinOnce"/> has been called.
        /// </summary>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// Gets a value that indicates whether the next call to <see cref="SpinOnce"/> will yield.
        /// </summary>
        public bool NextSpinWillYield
        {
            get
            {
                return _isSingleCpu || _count >= _yieldInterval;
            }
        }

        /// <summary>
        /// Performs a single spin.
        /// </summary>
        public void SpinOnce()
        {
            if( NextSpinWillYield )
            {
                Thread.Sleep(0);
            }
            else
            {
                Thread.SpinWait(_count);
            }
            _count = _count == Int32.MaxValue ? _yieldInterval : _count;
        }
    }
}
