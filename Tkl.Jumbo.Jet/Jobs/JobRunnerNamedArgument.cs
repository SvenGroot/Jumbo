using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a named command line argument for a job runner.
    /// </summary>
    public class JobRunnerNamedArgument : JobRunnerArgument
    {
        private PropertyInfo _property;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerNamedArgument"/> class.
        /// </summary>
        /// <param name="property">The property holding the named argument's value.</param>
        public JobRunnerNamedArgument(PropertyInfo property)
            : base(GetArgumentName(property), property.PropertyType)
        {
            NamedArgumentAttribute attribute = (NamedArgumentAttribute)Attribute.GetCustomAttribute(property, typeof(NamedArgumentAttribute));
            if( attribute == null )
                throw new ArgumentException("Specified property is not a named argument.", "property");
            Description = attribute.Description;
            _property = property;
        }

        /// <summary>
        /// Gets the description of the argument.
        /// </summary>
        public string Description { get; private set; }

        /// <summary>
        /// Gets the property name of the argument.
        /// </summary>
        public string PropertyName
        {
            get { return _property.Name; }
        }

        /// <summary>
        /// Gets or sets the value of the argument.
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Sets the argument value.
        /// </summary>
        /// <param name="target">The object whose argument should be set.</param>
        public void ApplyValue(object target)
        {
            if( target == null )
                throw new ArgumentNullException("target");

            if( Value != null )
                _property.SetValue(target, Value, null);
        }

        private static string GetArgumentName(PropertyInfo property)
        {
            if( property == null )
                throw new ArgumentNullException("property");

            NamedArgumentAttribute attribute = (NamedArgumentAttribute)Attribute.GetCustomAttribute(property, typeof(NamedArgumentAttribute));
            if( attribute == null )
                throw new ArgumentException("Specified property is not a named argument.", "property");
            return attribute.ArgumentName;
        }
    }
}
