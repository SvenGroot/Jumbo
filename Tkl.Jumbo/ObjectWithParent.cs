// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Base class for objects that have a parent that will be tracked by a <see cref="ChildCollection{TParent,TChild}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the parent.</typeparam>
    public class ObjectWithParent<T>
        where T : class
    {
        // I would've preferred for this to be an interface, but since the set method needs to be internal that's not possible.

        /// <summary>
        /// Gets the parent of this instance.
        /// </summary>
        [XmlIgnore]
        public T Parent { get; internal set; }
    }
}
