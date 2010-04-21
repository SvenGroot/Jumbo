// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Test.Tasks
{
    /// <summary>
    /// Provides <see cref="Order"/> comparisons based on <see cref="Order.CustomerId"/> which is needed for the join.
    /// </summary>
    public class OrderJoinComparer : IComparer<Order>, IEqualityComparer<Order>
    {
        #region IComparer<Order> Members

        public int Compare(Order x, Order y)
        {
            return x.CustomerId - y.CustomerId;
        }

        #endregion

        #region IEqualityComparer<Order> Members

        public bool Equals(Order x, Order y)
        {
            if( x == y )
                return true;
            else if( x == null || y == null )
                return false;
            else
                return x.CustomerId == y.CustomerId;
        }

        public int GetHashCode(Order obj)
        {
            return obj.CustomerId.GetHashCode();
        }

        #endregion
    }
}
