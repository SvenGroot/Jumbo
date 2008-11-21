using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides extension methods.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Copies one stream to another.
        /// </summary>
        /// <param name="src">The stream to copy from.</param>
        /// <param name="dest">The stream to copy to.</param>
        public static void CopyTo(this Stream src, Stream dest)
        {
            CopyTo(src, dest, 4096);
        }

        /// <summary>
        /// Copies one stream to another using the specified buffer size.
        /// </summary>
        /// <param name="src">The stream to copy from.</param>
        /// <param name="dest">The stream to copy to.</param>
        /// <param name="bufferSize">The size of the buffer to use while copying.</param>
        public static void CopyTo(this Stream src, Stream dest, int bufferSize)
        {
            if( src == null )
                throw new ArgumentNullException("src");
            if( dest == null )
                throw new ArgumentNullException("dest");
            byte[] buffer = new byte[bufferSize];
            int bytesRead = 0;
            do
            {
                bytesRead = src.Read(buffer, 0, buffer.Length);
                if( bytesRead > 0 )
                {
                    dest.Write(buffer, 0, bytesRead);
                }
            } while( bytesRead > 0 );
        }

        /// <summary>
        /// Copies the specified number of bytes from one stream to another using the specified buffer size.
        /// </summary>
        /// <param name="src">The stream to copy from.</param>
        /// <param name="dest">The stream to copy to.</param>
        /// <param name="size">The total number of bytes to copy.</param>
        public static void CopySize(this Stream src, Stream dest, long size)
        {
            CopySize(src, dest, size, 4096);
        }

        /// <summary>
        /// Copies the specified number of bytes from one stream to another using the specified buffer size.
        /// </summary>
        /// <param name="src">The stream to copy from.</param>
        /// <param name="dest">The stream to copy to.</param>
        /// <param name="size">The total number of bytes to copy.</param>
        /// <param name="bufferSize">The size of the buffer to use while copying.</param>
        public static void CopySize(this Stream src, Stream dest, long size, int bufferSize)
        {
            if( src == null )
                throw new ArgumentNullException("src");
            if( dest == null )
                throw new ArgumentNullException("dest");
            byte[] buffer = new byte[bufferSize];
            long bytesLeft = size;
            do
            {
                int bytesRead = src.Read(buffer, 0, (int)Math.Min(buffer.Length, bytesLeft));
                if( bytesRead == 0 )
                {
                    throw new EndOfStreamException("Reached end of stream before specified number of bytes was copied.");
                }
                dest.Write(buffer, 0, bytesRead);
                bytesLeft -= size;
            } while( bytesLeft > 0 );
        }
    }
}
