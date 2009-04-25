using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Indicates the property can be set via a named command line argument.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class NamedArgumentAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NamedArgumentAttribute"/> class.
        /// </summary>
        /// <param name="argumentName">the name of the argument's command switch.</param>
        public NamedArgumentAttribute(string argumentName)
        {
            if( argumentName == null )
                throw new ArgumentNullException("argumentName");

            ArgumentName = argumentName;
        }

        /// <summary>
        /// Gets the name of the argument's command switch.
        /// </summary>
        public string ArgumentName { get; private set; }

        /// <summary>
        /// Gets or sets the description of the argument.
        /// </summary>
        public string Description { get; set; }
    }
}
