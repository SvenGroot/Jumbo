using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Runtime.ConstrainedExecution;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides a mutual exclusion lock primitive where a thread trying to acquire the lock waits in a loop repeatedly checking until the lock becomes available.
    /// </summary>
    /// <remarks>
    /// The public API is based on .Net 4.0's SpinWait class so once Mono supports that we can switch if desired.
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1815:OverrideEqualsAndOperatorEqualsOnValueTypes"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "SpinLock")]
    public struct SpinLock
    {
        private volatile int _owner;
        private bool _isThreadOwnerTrackingEnabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="SpinLock"/> structure.
        /// </summary>
        /// <param name="enableOwnerTracking"><see langword="true"/> to enable owner tracking; otherwise, <see langword="false"/>.</param>
        public SpinLock(bool enableOwnerTracking)
        {
            _owner = 0;
            _isThreadOwnerTrackingEnabled = enableOwnerTracking;
        }

        /// <summary>
        /// Gets a value that indicates whether the current thread holds the lock.
        /// </summary>
        public bool IsHeldByCurrentThread
        {
            get { return _isThreadOwnerTrackingEnabled ? _owner == Thread.CurrentThread.ManagedThreadId : _owner == 1; }
        }

        /// <summary>
        /// Gets a value that indicates whether thread owner tracking is enabled.
        /// </summary>
        public bool IsThreadOwnerTrackingEnabled
        {
            get { return _isThreadOwnerTrackingEnabled; }
        }

        /// <summary>
        /// Acquires the lock in a reliable manner.
        /// </summary>
        /// <param name="lockTaken"><see langword="true"/> if the lock was acquired; otherwise, <see langword="false"/>.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1045:DoNotPassTypesByReference", MessageId = "0#"), ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        public void Enter(ref bool lockTaken)
        {
            TryEnter(Timeout.Infinite, ref lockTaken);
        }

        /// <summary>
        /// Attempts to acquire the lock in a reliable manner.
        /// </summary>
        /// <param name="lockTaken"><see langword="true"/> if the lock was acquired; otherwise, <see langword="false"/>.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1045:DoNotPassTypesByReference", MessageId = "0#"), ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        public void TryEnter(ref bool lockTaken)
        {
            TryEnter(0, ref lockTaken);
        }

        /// <summary>
        /// Attempts to acquire the lock in a reliable manner.
        /// </summary>
        /// <param name="millisecondsTimeout">The number of milliseconds to wait, or <see langword="Timeout.Infinite"/> to wait forever.</param>
        /// <param name="lockTaken"><see langword="true"/> if the lock was acquired; otherwise, <see langword="false"/>.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1045:DoNotPassTypesByReference", MessageId = "1#"), ReliabilityContract(Consistency.WillNotCorruptState, Cer.MayFail)]
        public void TryEnter(int millisecondsTimeout, ref bool lockTaken)
        {
            if( millisecondsTimeout < -1 )
                throw new ArgumentOutOfRangeException("millisecondsTimeout");

            // lockTaken is a ref parameter, not a return value, so its value is still usable even if an exception occurred (e.g. ThreadAbortException) in this method.
            if( lockTaken )
            {
                lockTaken = false;
                throw new ArgumentException("lockTaken must be false.", "lockTaken");
            }

            int newOwner;
            if( _isThreadOwnerTrackingEnabled )
            {
                newOwner = Thread.CurrentThread.ManagedThreadId;
                if( newOwner == _owner )
                    throw new LockRecursionException("Lock already held by current thread.");
            }
            else
                newOwner = 1;

            Stopwatch timeout = millisecondsTimeout > 0 ? null : Stopwatch.StartNew();
            SpinWait wait = new SpinWait();

            while( true )
            {
                int owner = _owner;
                if( owner == 0 )
                {
                    try
                    {
                    }
                    finally
                    {
                        // This is done inside a finally block because ThreadAbortExceptions are postponed until after the finally block finishes, allowing
                        // reliable completion of this code.
#pragma warning disable 0420 // volatile field not treated as volatile.
                        if( Interlocked.CompareExchange(ref _owner, newOwner, owner) == owner )
                            lockTaken = true;
#pragma warning restore 0420
                    }
                    if( lockTaken )
                        return;
                }

                if( millisecondsTimeout == 0 || millisecondsTimeout != -1 && timeout.ElapsedMilliseconds > millisecondsTimeout )
                {
                    return;
                }
                wait.SpinOnce();
            }
        }

        /// <summary>
        /// Releases the lock.
        /// </summary>
        public void Exit()
        {
            Exit(false);
        }

        /// <summary>
        /// Releases the lock.
        /// </summary>
        /// <param name="flushReleaseWrite">A Boolean value that indicates whether a memory fence should be issued in order to immediately publish the exit operation to other threads.</param>
        public void Exit(bool flushReleaseWrite)
        {
            if( !IsHeldByCurrentThread )
                throw new SynchronizationLockException("Lock not held by current thread.");

            int newOwner = 0;
#pragma warning disable 0420 // volatile field not treated as volatile.
            if( flushReleaseWrite )
                Interlocked.Exchange(ref _owner, newOwner); // Use of interlocked instruction causes cache flush.
            else
                _owner = newOwner;
#pragma warning restore 0420
        }
    }
}
