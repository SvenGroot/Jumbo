using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// A paritioner based on the value returned by <see cref="Object.GetHashCode"/>.
    /// </summary>
    /// <typeparam name="T">The type of the values to partition.</typeparam>
    public class HashPartitioner<T> : IPartitioner<T>
    {
        #region IPartitioner<T> Members

        /// <summary>
        /// Gets or sets the number of partitions.
        /// </summary>
        public int Partitions { get; set; }

        /// <summary>
        /// Gets the partition for the specified value.
        /// </summary>
        /// <param name="value">The value to be partitioned.</param>
        /// <returns>The partition number for the specified value.</returns>
        public int GetPartition(T value)
        {
            if( value == null )
                return 0;
            else
                return (value.GetHashCode() & int.MaxValue) % Partitions;
        }

        #endregion
    }
}
