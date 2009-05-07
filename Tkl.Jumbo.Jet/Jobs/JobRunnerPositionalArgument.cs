using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents a positional command line argument for a job runner.
    /// </summary>
    public class JobRunnerPositionalArgument : JobRunnerArgument
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerPositionalArgument"/> class.
        /// </summary>
        /// <param name="name">The name of the argument.</param>
        /// <param name="argumentType">The type of the argument</param>
        /// <param name="optional"><see langword="true"/> if the argument is optional; otherwise, <see langword="false"/>.</param>
        /// <param name="defaultValue">The default value if the argument is optional, ignored otherwise.</param>
        /// <param name="description">A description for the argument, or <see langword="null"/> if the argument has no description.</param>
        public JobRunnerPositionalArgument(string name, Type argumentType, bool optional, object defaultValue, string description)
            : base(name, argumentType, description)
        {
            if( optional )
            {
                if( (defaultValue == null && !argumentType.IsClass) || (defaultValue != null && defaultValue.GetType() != argumentType) )
                    throw new ArgumentException("Default value does not have the same type as the argument.");
            }

            IsOptional = optional;
            DefaultValue = defaultValue;
        }

        /// <summary>
        /// Gets a value that indicates whether the argument is optional.
        /// </summary>
        public bool IsOptional { get; private set; }

        /// <summary>
        /// Gets the default value of the argument if it is optional.
        /// </summary>
        public object DefaultValue { get; private set; }
    }
}
