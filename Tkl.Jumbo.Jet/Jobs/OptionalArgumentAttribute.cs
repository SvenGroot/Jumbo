using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Indicates the specified argument is optional.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class OptionalArgumentAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OptionalArgumentAttribute"/> class.
        /// </summary>
        /// <param name="defaultValue">The default value of the argument.</param>
        public OptionalArgumentAttribute(object defaultValue)
        {
            DefaultValue = defaultValue;
        }

        /// <summary>
        /// Gets the default value of the parameter.
        /// </summary>
        public object DefaultValue { get; private set; }
    }
}
