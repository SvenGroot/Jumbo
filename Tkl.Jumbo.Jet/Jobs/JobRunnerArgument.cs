using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a command line argument for a job runner.
    /// </summary>
    public class JobRunnerArgument
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerArgument"/> class.
        /// </summary>
        /// <param name="name">The name of the argument.</param>
        /// <param name="argumentType">The type of the argument</param>
        /// <param name="optional"><see langword="true"/> if the argument is optional; otherwise, <see langword="false"/>.</param>
        /// <param name="defaultValue">The default value if the argument is optional, ignored otherwise.</param>
        public JobRunnerArgument(string name, Type argumentType, bool optional, object defaultValue)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( argumentType == null )
                throw new ArgumentNullException("argumentType");
            if( optional )
            {
                if( (defaultValue == null && !argumentType.IsClass) || (defaultValue != null && defaultValue.GetType() != argumentType) )
                    throw new ArgumentException("Default value does not have the same type as the argument.");
            }

            Name = name;
            ArgumentType = argumentType;
            IsOptional = optional;
            DefaultValue = defaultValue;
        }

        /// <summary>
        /// Gets the name of the argument.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the type of the argument.
        /// </summary>
        public Type ArgumentType { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether the argument is optional.
        /// </summary>
        public bool IsOptional { get; private set; }

        /// <summary>
        /// Gets the default value of the argument if it is optional.
        /// </summary>
        public object DefaultValue { get; private set; }

        /// <summary>
        /// Converts the specified string to the argument type.
        /// </summary>
        /// <param name="argument">The string to convert.</param>
        /// <returns>The converted argument.</returns>
        public object ConvertToArgumentType(string argument)
        {
            try
            {
                if( ArgumentType.IsEnum )
                    return Enum.Parse(ArgumentType, argument);
                else
                    return Convert.ChangeType(argument, ArgumentType);
            }
            catch( InvalidCastException ex )
            {
                throw new InvalidCastException(string.Format("Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
            catch( FormatException ex )
            {
                throw new InvalidCastException(string.Format("Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
            catch( ArgumentException ex )
            {
                throw new InvalidCastException(string.Format("Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
        }
    }
}
