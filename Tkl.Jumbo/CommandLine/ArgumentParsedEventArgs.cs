using System;
using System.ComponentModel;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Provides data for the <see cref="CommandLineParser.ArgumentParsed"/> event.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   If the event handler sets the <see cref="CancelEventArgs.Cancel"/> property to <see langword="true"/>, command line processing will stop immediately,
    ///   and the <see cref="CommandLineParser.Parse"/> method will return <see langword="null"/>, even if all the required positional parameters have already
    ///   been parsed. You can use this for instance to implement a "/?" argument that will display usage and quit regardless of the other command line arguments.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false"/>
    public class ArgumentParsedEventArgs : CancelEventArgs
    {
        private readonly CommandLineArgument _argument;
        private readonly object _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentParsedEventArgs"/> class.
        /// </summary>
        /// <param name="argument">The information about the argument that has been parsed.</param>
        /// <param name="value">The parsed value of the argument. May be <see langword="null"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="argument"/> is <see langword="null"/>.</exception>
        public ArgumentParsedEventArgs(CommandLineArgument argument, object value)
        {
            if( argument == null )
                throw new ArgumentNullException("argument");

            _argument = argument;
            _value = value;
        }

        /// <summary>
        /// Gets the information about the argument that was parsed.
        /// </summary>
        /// <value>
        /// The <see cref="CommandLineArgument"/> instance describing the argument.
        /// </value>
        public CommandLineArgument Argument
        {
            get { return _argument; }
        }

        /// <summary>
        /// Gets the value of the argument.
        /// </summary>
        /// <value>
        /// The value of the argument.
        /// </value>
        public object Value
        {
            get { return _value; }
        }
    }
}
