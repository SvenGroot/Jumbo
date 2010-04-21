// $Id$
//
using System;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Indicates a property of a class can be set via a named command line argument.
    /// </summary>
    /// <threadsafety static="true" instance="false"/>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1813:AvoidUnsealedAttributes"), AttributeUsage(AttributeTargets.Property)]
    public class NamedCommandLineArgumentAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NamedCommandLineArgumentAttribute"/> class.
        /// </summary>
        /// <param name="argumentName">the name of the argument's command switch.</param>
        /// <exception cref="ArgumentNullException"><paramref name="argumentName"/> is <see langword="null"/>.</exception>
        public NamedCommandLineArgumentAttribute(string argumentName)
        {
            if( argumentName == null )
                throw new ArgumentNullException("argumentName");

            ArgumentName = argumentName;
        }

        /// <summary>
        /// Gets the name of the argument's command switch.
        /// </summary>
        /// <value>
        /// The name of the command switch used to set the argument.
        /// </value>
        public string ArgumentName { get; private set; }

        /// <summary>
        /// Gets or sets the default value to be assigned to the property if the argument is not specified on the command line.
        /// </summary>
        /// <value>
        /// The default value of the argument.
        /// </value>
        public object DefaultValue { get; set; }
    }
}
