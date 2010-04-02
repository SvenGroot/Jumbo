using System;
using System.Reflection;
using System.ComponentModel;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Provides information about a named command line argument.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Named command line arguments are identified by the name of the command line switch. If
    ///   an application is invoked with "ExecutableName.exe /arg1:1 /arg2 arg3 arg4", then arg1 and arg2
    ///   are named arguments.
    /// </para>
    /// <para>
    ///   The value of a named argument is specified after the argument name on the command line, separated by
    ///   a colon. In the example above, the value of arg1 is 1.
    /// </para>
    /// <para>
    ///   For <see cref="Boolean"/> properties, the value is determined simply by the presence of the argument.
    ///   If the argument is not present on the command line, the value will be <see langword="false"/>. If the
    ///   argument is present, the value will be <see langword="true"/>.
    /// </para>
    /// <para>
    ///   Named command line arguments correspond to properties of the class containing the command line arguments
    ///   that have the <see cref="NamedCommandLineArgumentAttribute"/> attribute.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false"/>
    public sealed class NamedCommandLineArgument : CommandLineArgument
    {
        private PropertyInfo _property;

        internal NamedCommandLineArgument(PropertyInfo property)
            : base(GetArgumentName(property), property.PropertyType, GetDescription(property), GetDefaultValue(property))
        {
            _property = property;
        }

        /// <summary>
        /// Gets the name of the property that declares the argument.
        /// </summary>
        /// <value>
        /// The name of the property that declares the argument.
        /// </value>
        public string PropertyName
        {
            get { return _property.Name; }
        }

        internal void ApplyValue(object target)
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

            NamedCommandLineArgumentAttribute attribute = GetNamedCommandLineArgumentAttribute(property);
            return attribute.ArgumentName;
        }

        private static string GetDescription(PropertyInfo property)
        {
            if( property == null )
                throw new ArgumentNullException("property");

            DescriptionAttribute attribute = (DescriptionAttribute)Attribute.GetCustomAttribute(property, typeof(DescriptionAttribute));
            return attribute == null ? null : attribute.Description;
        }

        private static object GetDefaultValue(PropertyInfo property)
        {
            NamedCommandLineArgumentAttribute attribute = GetNamedCommandLineArgumentAttribute(property);
            return attribute.DefaultValue;
        }

        private static NamedCommandLineArgumentAttribute GetNamedCommandLineArgumentAttribute(PropertyInfo property)
        {
            NamedCommandLineArgumentAttribute attribute = (NamedCommandLineArgumentAttribute)Attribute.GetCustomAttribute(property, typeof(NamedCommandLineArgumentAttribute));
            if( attribute == null )
                throw new ArgumentException(Properties.Resources.MissingNamedArgumentAttribute, "property");
            return attribute;
        }
    }
}
