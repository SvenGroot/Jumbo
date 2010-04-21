using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a command line argument for a job runner.
    /// </summary>
    public abstract class JobRunnerArgument
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerArgument"/> class.
        /// </summary>
        /// <param name="name">The name of the argument.</param>
        /// <param name="argumentType">The type of the argument</param>
        /// <param name="description">The description of the argument, or <see langword="null"/> if the argument has no description.</param>
        protected JobRunnerArgument(string name, Type argumentType, string description)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( argumentType == null )
                throw new ArgumentNullException("argumentType");

            Name = name;
            ArgumentType = argumentType;
            Description = description;
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
        /// Gets the description of the argument.
        /// </summary>
        public string Description { get; private set; }

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
                    return Convert.ChangeType(argument, ArgumentType, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch( InvalidCastException ex )
            {
                throw new InvalidCastException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
            catch( FormatException ex )
            {
                throw new InvalidCastException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
            catch( ArgumentException ex )
            {
                throw new InvalidCastException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Could not convert the value of argument {0} to type {1}.", Name, ArgumentType.FullName), ex);
            }
        }
    }
}
