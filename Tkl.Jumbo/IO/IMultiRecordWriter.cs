﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Interface for record writers that use partitioning.
    /// </summary>
    public interface IMultiRecordWriter<T>
    {
        /// <summary>
        /// Gets the partitioner.
        /// </summary>
        /// <value>The partitioner.</value>
        IPartitioner<T> Partitioner { get; }
    }
}
