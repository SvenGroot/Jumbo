using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Tkl.Jumbo.NetworkTopology
{
    /// <summary>
    /// Represents a collection of <see cref="RackConfigurationElement"/> objects in a configuration file.
    /// </summary>
    public class RackConfigurationElementCollection : ConfigurationElementCollection
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RackConfigurationElementCollection"/> class.
        /// </summary>
        public RackConfigurationElementCollection()
        {
            AddElementName = "rack";
        }

        /// <summary>
        /// Creates a new element.
        /// </summary>
        /// <returns>A new <see cref="RackConfigurationElement"/>.</returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new RackConfigurationElement();
        }

        /// <summary>
        /// Gets the element key.
        /// </summary>
        /// <param name="element">The element whose key to get.</param>
        /// <returns>The <see cref="RackConfigurationElement.RackId"/> property value.</returns>
        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((RackConfigurationElement)element).RackId;
        }

        /// <summary>
        /// Gets the element in the collection at the specified index.
        /// </summary>
        /// <param name="index">The index of the item to return.</param>
        /// <returns>The item at the specified index.</returns>
        public RackConfigurationElement this[int index]
        {
            get { return (RackConfigurationElement)BaseGet(index); }
        }

    }
}
