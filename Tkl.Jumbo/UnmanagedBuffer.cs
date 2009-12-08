﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Represents a buffer of memory stored on the unmanaged (native) heap.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This class is used as an alternative for byte[]. The main differences are that the memory is returned
    ///   to the OS when the class is disposed/finalized, and the buffer is not zero-initialized.
    /// </para>
    /// <para>
    ///   The main reason for the existence of this class is Mono's reluctance to release memory from the managed heap
    ///   back to the OS which can lead to pagefile thrashing if you're dealing with many large buffers.
    /// </para>
    /// </remarks>
    public unsafe sealed class UnmanagedBuffer : IDisposable
    {
        private byte* _buffer;

        /// <summary>
        /// Initializes a new instance of the <see cref="UnmanagedBuffer"/> class.
        /// </summary>
        /// <param name="size">The size, in bytes, of the buffer.</param>
        public UnmanagedBuffer(int size)
        {
            _buffer = (byte*)Marshal.AllocHGlobal(size);
            Size = size;
        }

        /// <summary>
        /// Releases all resources used by this class.
        /// </summary>
        ~UnmanagedBuffer()
        {
            DisposeInternal();
        }

        /// <summary>
        /// Gets the size of the buffer, in bytes.
        /// </summary>
        public int Size { get; private set; }

        /// <summary>
        /// Gets a pointer to the first byte of the buffer.
        /// </summary>
        [CLSCompliant(false)]
        public byte* Buffer
        {
            get { return _buffer; }
        }

        /// <summary>
        /// Copies data from a managed array to the buffer.
        /// </summary>
        /// <param name="source">The managed byte array containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The <see cref="UnmanagedBuffer"/> to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        public static void Copy(byte[] source, int sourceIndex, UnmanagedBuffer destination, int destinationIndex, int count)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            if( destination == null )
                throw new ArgumentNullException("destination");
            if( sourceIndex < 0 )
                throw new ArgumentOutOfRangeException("sourceIndex");
            if( destinationIndex < 0 )
                throw new ArgumentOutOfRangeException("destinationIndex");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( sourceIndex + count > source.Length )
                throw new ArgumentException("sourceIndex + count is larger than the source array.");
            if( destinationIndex + count > destination.Size )
                throw new ArgumentException("destinationIndex + count is larger than the destination array.");

            destination.CheckDisposed();

            Marshal.Copy(source, sourceIndex, new IntPtr(destination._buffer + destinationIndex), count);
        }

        /// <summary>
        /// Copies data from a buffer to a managed array.
        /// </summary>
        /// <param name="source">The <see cref="UnmanagedBuffer"/> containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The managed byte array to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        public static void Copy(UnmanagedBuffer source, int sourceIndex, byte[] destination, int destinationIndex, int count)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            if( destination == null )
                throw new ArgumentNullException("destination");
            if( sourceIndex < 0 )
                throw new ArgumentOutOfRangeException("sourceIndex");
            if( destinationIndex < 0 )
                throw new ArgumentOutOfRangeException("destinationIndex");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( sourceIndex + count > source.Size )
                throw new ArgumentException("sourceIndex + count is larger than the source array.");
            if( destinationIndex + count > destination.Length )
                throw new ArgumentException("destinationIndex + count is larger than the destination array.");

            source.CheckDisposed();

            Marshal.Copy(new IntPtr(source._buffer + sourceIndex), destination, destinationIndex, count);
        }

        /// <summary>
        /// Resizes the buffer.
        /// </summary>
        /// <param name="size">The new size of the buffer.</param>
        public void Resize(int size)
        {
            CheckDisposed();
            _buffer = (byte*)Marshal.ReAllocHGlobal(new IntPtr(_buffer), new IntPtr(size));
        }

        /// <summary>
        /// Releases all resources used by this class.
        /// </summary>
        public void Dispose()
        {
            DisposeInternal();
            GC.SuppressFinalize(this);
        }

        private void DisposeInternal()
        {
            if( _buffer != null )
            {
                Marshal.FreeHGlobal(new IntPtr(_buffer));
                _buffer = null;
            }
        }

        private void CheckDisposed()
        {
            if( _buffer == null )
                throw new ObjectDisposedException(typeof(UnmanagedBuffer).FullName);
        }
    }
}