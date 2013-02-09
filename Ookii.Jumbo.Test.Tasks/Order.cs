// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    public class Order : Writable<Order>, ICloneable
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
            return Id.GetHashCode();
        }

        #region ICloneable Members

        public object Clone()
        {
            return MemberwiseClone();
        }

        #endregion
    }
}
