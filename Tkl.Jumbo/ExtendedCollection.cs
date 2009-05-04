﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Provides extension methods for <see cref="Collection{T}"/>.
    /// </summary>
    [Serializable]
    public class ExtendedCollection<T> : Collection<T>
    {
        private readonly List<T> _itemsList;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExtendedCollection{T}"/> class.
        /// </summary>
        public ExtendedCollection()
            : base(new List<T>())
        {
            _itemsList = (List<T>)Items;
        }

        /// <summary>
        /// Adds a range of elements to the collection.
        /// </summary>
        /// <param name="collection">The collection containing the elements to add.</param>
        public void AddRange(IEnumerable<T> collection)
        {
            _itemsList.AddRange(collection);
        }
    }
}
