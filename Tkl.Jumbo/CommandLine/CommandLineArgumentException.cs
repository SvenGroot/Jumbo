// $Id$
//
using System;
using System.Security.Permissions;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// The exception that is thrown when one of the command line arguments is not valid.
    /// </summary>
    /// <threadsafety static="true" instance="false"/>
    [Serializable]
    public class CommandLineArgumentException : Exception
    {
        private readonly string _argumentName;

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class. 
        /// </summary>
        public CommandLineArgumentException() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CommandLineArgumentException(string message) : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="argumentName">The name of the argument that was invalid.</param>
        public CommandLineArgumentException(string message, string argumentName)
            : base(message)
        {
            _argumentName = argumentName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class with a specified error message and a reference to the inner <see cref="Exception"/> that is the cause of this <see cref="CommandLineArgumentException"/>. 
        /// </summary>
        /// <param name="message">The error message that explains the reason for the <see cref="CommandLineArgumentException"/>.</param>
        /// <param name="inner">The <see cref="Exception"/> that is the cause of the current <see cref="CommandLineArgumentException"/>, or a <see langword="null"/> if no inner <see cref="Exception"/> is specified.</param>
        public CommandLineArgumentException(string message, Exception inner) : base(message, inner) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class with a specified error message and a reference to the inner <see cref="Exception"/> that is the cause of this <see cref="CommandLineArgumentException"/>. 
        /// </summary>
        /// <param name="message">The error message that explains the reason for the <see cref="CommandLineArgumentException"/>.</param>
        /// <param name="argumentName">The name of the argument that was invalid.</param>
        /// <param name="inner">The <see cref="Exception"/> that is the cause of the current <see cref="CommandLineArgumentException"/>, or a <see langword="null"/> if no inner <see cref="Exception"/> is specified.</param>
        public CommandLineArgumentException(string message, string argumentName, Exception inner)
            : base(message, inner)
        {
            _argumentName = argumentName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgumentException"/> class with serialized data. 
        /// </summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the <see cref="CommandLineArgumentException"/> being thrown.</param>
        /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected CommandLineArgumentException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) 
        {
            _argumentName = info.GetString("ArgumentName");
        }

        /// <summary>
        /// Gets the name of the argument that was invalid.
        /// </summary>
        /// <value>
        /// The name of the invalid argument.
        /// </value>
        public string ArgumentName
        {
            get { return _argumentName; }
        }

        /// <summary>
        /// Sets the <see cref="System.Runtime.Serialization.SerializationInfo"/> object with the parameter name and additional exception information.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data.</param>
        /// <param name="context">The contextual information about the source or destination.</param>
        /// <exception cref="ArgumentNullException"><paramref name="info"/> is <see langword="null"/>.</exception>
        [SecurityPermission(SecurityAction.LinkDemand, Flags = SecurityPermissionFlag.SerializationFormatter)]
        public override void GetObjectData(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
        {
            if( info == null )
                throw new ArgumentNullException("info");
            base.GetObjectData(info, context);

            info.AddValue("ArgumentName", ArgumentName);
        }
    }
}
