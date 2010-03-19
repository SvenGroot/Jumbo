using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Attribute for use with types inheriting from <see cref="ShellCommand"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class ShellCommandAttribute : Attribute
    {
        private readonly string _commandName;

        /// <summary>
        /// Initializes a new instance of the <see cref="ShellCommandAttribute"/> class.
        /// </summary>
        /// <param name="commandName">The name of the command used to invoke it.</param>
        public ShellCommandAttribute(string commandName)
        {
            if( commandName == null )
                throw new ArgumentNullException("commandName");

            _commandName = commandName;
        }

        /// <summary>
        /// Gets the name of the command used to invoke it.
        /// </summary>
        public string CommandName
        {
            get { return _commandName; }
        }
    }
}
