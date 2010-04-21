using System;
using System.Reflection;
using System.ComponentModel;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Provides information about a positional command line argument.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Positional command line arguments are identified by the order in which they appear on the command line. If
    ///   an application is invoked with "ExecutableName.exe /arg1:1 /arg2 arg3 arg4", then arg3 and arg4
    ///   are positional arguments.
    /// </para>
    /// <para>
    ///   Positional arguments can be optional, in which case they may be omitted. Once one of the arguments is optional,
    ///   all arguments following that argument's position must also be optional.
    /// </para>
    /// <para>
    ///   Positional command line arguments correspond to the parameters of the constructor of the class containing the command line
    ///   arguments. If the class has more than one constructor, the constructor that has the <see cref="CommandLineConstructorAttribute"/>
    ///   attribute will be used.
    /// </para>
    /// <para>
    ///   To create a optional argument, apply the <see cref="System.Runtime.InteropServices.OptionalAttribute"/> attribute to the constructor parameter.
    ///   To set the default value, apply the <see cref="System.Runtime.InteropServices.DefaultParameterValueAttribute"/> to the constructor parameter.
    ///   In Visual Basic, you can use the <see langword="Optional"/> keyword and built-in syntax to specify the default value.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false" />
    public sealed class PositionalCommandLineArgument : CommandLineArgument
    {
        internal PositionalCommandLineArgument(ParameterInfo parameter)
            : base(parameter.Name, parameter.ParameterType, GetDescription(parameter), ((parameter.Attributes & ParameterAttributes.HasDefault) == ParameterAttributes.HasDefault) ? parameter.DefaultValue : null)
        {
            IsOptional = parameter.IsOptional;
        }

        /// <summary>
        /// Gets a value that indicates whether the argument is optional.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the argument's value may be omitted from the command line; <see langword="false"/> if the argument must be included.
        /// </value>
        public bool IsOptional { get; private set; }

        private static string GetDescription(ParameterInfo parameter)
        {
            DescriptionAttribute attribute = (DescriptionAttribute)Attribute.GetCustomAttribute(parameter, typeof(DescriptionAttribute));
            return attribute == null ? null : attribute.Description;
        }    
    }
}
