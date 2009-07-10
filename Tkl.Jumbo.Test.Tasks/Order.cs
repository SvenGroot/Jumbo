using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Test.Tasks
{
    public class Order : Writable<Order>, IComparable<Order>, ICloneable
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public int ItemId { get; set; }

        public override bool Equals(object obj)
        {
            Order other = obj as Order;
            if( other == null )
                return false;
            return Id == other.Id && CustomerId == other.CustomerId && ItemId == other.ItemId;
        }

        public override int GetHashCode()
        {
            // This is again for join purposes.
            return CustomerId.GetHashCode();
        }

        #region ICloneable Members

        public object Clone()
        {
            return MemberwiseClone();
        }

        #endregion

        #region IComparable<Order> Members

        public int CompareTo(Order other)
        {
            // This is for purposes of the join.
            return CustomerId - other.CustomerId;
        }

        #endregion
    }
}
