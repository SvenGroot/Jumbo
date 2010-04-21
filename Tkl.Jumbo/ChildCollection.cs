// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides a collection of child objects that keep a reference to their parent.
    /// </summary>
    /// <typeparam name="TParent">The type of the parent.</typeparam>
    /// <typeparam name="TChild">The type of the children.</typeparam>
    [Serializable]
    public class ChildCollection<TParent, TChild> : ExtendedCollection<TChild>
        where TParent : class
        where TChild : ObjectWithParent<TParent>
    {
        private readonly TParent _parent;

        /// <summary>
        /// Initializes a new instance of the <see cref="ChildCollection{TParent, TChild}"/> class.
        /// </summary>
        /// <param name="parent">The parent of the collection.</param>
        public ChildCollection(TParent parent)
        {
            if( parent == null )
                throw new ArgumentNullException("parent");
            _parent = parent;
        }

        /// <summary>
        /// Overrides <see cref="Collection{T}.ClearItems"/>.
        /// </summary>
        protected override void ClearItems()
        {
            foreach( TChild item in this )
                item.Parent = null;
            base.ClearItems();
        }

        /// <summary>
        /// Overrides <see cref="Collection{T}.InsertItem"/>.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        protected override void InsertItem(int index, TChild item)
        {
            if( item.Parent != null )
                throw new ArgumentException("The item already has a parent.", "item");
            base.InsertItem(index, item);
            item.Parent = _parent;
        }

        /// <summary>
        /// Overrides <see cref="Collection{T}.RemoveItem"/>.
        /// </summary>
        /// <param name="index"></param>
        protected override void RemoveItem(int index)
        {
            this[index].Parent = null;
            base.RemoveItem(index);
        }

        /// <summary>
        /// Overrides <see cref="Collection{T}.SetItem"/>.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        protected override void SetItem(int index, TChild item)
        {
            if( item.Parent != null )
                throw new ArgumentException("The item already has a parent.", "item");
            this[index].Parent = null;
            base.SetItem(index, item);
            item.Parent = _parent;
        }
    }
}
