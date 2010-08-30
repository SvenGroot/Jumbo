// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    sealed class PrepartitionedPartitioner<T> : IPartitioner<T>
    {
        private int _currentPartition;

        public int Partitions { get; set; }

        public int CurrentPartition
        {
            get { return _currentPartition; }
            set 
            {
                if( value < 0 || value >= Partitions )
                    throw new ArgumentOutOfRangeException("value");
                _currentPartition = value; 
            }
        }
        

        public int GetPartition(T value)
        {
            return CurrentPartition;
        }
    }
}
