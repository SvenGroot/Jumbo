using System;
using System.ComponentModel;
using System.Diagnostics;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Base class for information about named and positional command line arguments.
    /// </summary>
    /// <threadsafety static="true" instance="false"/>
    public abstract class CommandLineArgument
    {
        private readonly TypeConverter _converter;

        internal CommandLineArgument(string name, Type argumentType, string description, object defaultValue)
        {
            if( name == null )
                throw new ArgumentNullException("name");
            if( name.Length == 0 )
                throw new ArgumentException(Properties.Resources.EmptyArgumentName, "name");
            if( argumentType == null )
                throw new ArgumentNullException("argumentType");
            if( defaultValue != null && defaultValue.GetType() != argumentType )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.IncorrectDefaultValueTypeFormat, name));

            if( argumentType.IsArray )
            {
                if( argumentType.GetArrayRank() != 1 )
                    throw new ArgumentException(Properties.Resources.InvalidArrayRank, "argumentType");
                _converter = TypeDescriptor.GetConverter(argumentType.GetElementType());
            }
            else
                _converter = TypeDescriptor.GetConverter(argumentType);

            if( _converter == null || !_converter.CanConvertFrom(typeof(string)) )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.NoTypeConverterFormat, name, argumentType), "argumentType");

            Name = name;
            ArgumentType = argumentType;
            Description = description;
            DefaultValue = defaultValue;
        }

        /// <summary>
        /// Gets the name of the argument.
        /// </summary>
        /// <value>
        /// The name of the argument.
        /// </value>
        /// <remarks>
        /// <para>
        ///   For a positional argument, this property is used only when generating usage information using <see cref="CommandLineParser.Usage"/>.
        /// </para>
        /// <para>
        ///   For a named argument, this is the name of the switch used on the command line to set the argument. To get the name of the property that will receive
        ///   the argument's value, use the <see cref="NamedCommandLineArgument.PropertyName"/> property.
        /// </para>
        /// </remarks>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the type of the argument.
        /// </summary>
        /// <value>
        /// The <see cref="Type"/> of the argument.
        /// </value>
        public Type ArgumentType { get; private set; }

        /// <summary>
        /// Gets the description of the argument.
        /// </summary>
        /// <value>
        /// A short description of the argument.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This property is used only when generating usage information using <see cref="CommandLineParser.Usage"/>.
        /// </para>
        /// <para>
        ///   To set the description of an argument, apply the <see cref="System.ComponentModel.DescriptionAttribute"/> attribute to the constructor parameter of the argument (for
        ///   positional arguments), or the property of the argument (for named arguments).
        /// </para>
        /// </remarks>
        public string Description { get; private set; }

        /// <summary>
        /// Gets the default value for an argument.
        /// </summary>
        /// <value>
        /// The default value of the argument.
        /// </value>
        /// <remarks>
        /// <para>
        ///   For a positional argument, this value is only used if <see cref="PositionalCommandLineArgument.IsOptional"/> is <see langword="true"/>.
        /// </para>
        /// </remarks>
        public object DefaultValue { get; private set; }

        /// <summary>
        /// Gets the value of the argument in the last call to <see cref="CommandLineParser.Parse"/>.
        /// </summary>
        public object Value { get; internal set; }
        
        /// <summary>
        /// Converts the specified string to the argument type, as specified in the <see cref="ArgumentType"/> property.
        /// </summary>
        /// <param name="argument">The string to convert.</param>
        /// <returns>The argument, converted to the type specified by the <see cref="ArgumentType"/> property.</returns>
        /// <remarks>
        /// <para>
        ///   The <see cref="TypeConverter"/> for the type specified by <see cref="ArgumentType"/> is used to do the conversion.
        /// </para>
        /// </remarks>
        /// <exception cref="CommandLineArgumentException">One of the arguments did not have the proper format for the type of the argument.</exception>
        public object ConvertToArgumentType(string argument)
        {
            try
            {
                return _converter.ConvertFrom(argument);
            }
            catch( NotSupportedException ex )
            {
                // Yeah, I don't like catching Exception, but unfortunately some conversions (e.g. string to int) can *throw* a System.Exception (not a derived class) so there's nothing I can do about it.
                throw new CommandLineArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.ArgumentConversionErrorFormat, argument, Name, ArgumentType.Name), Name, ex);
            }
            catch( FormatException ex )
            {
                // Yeah, I don't like catching Exception, but unfortunately some conversions (e.g. string to int) can *throw* a System.Exception (not a derived class) so there's nothing I can do about it.
                throw new CommandLineArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.ArgumentConversionErrorFormat, argument, Name, ArgumentType.Name), Name, ex);
            }
            catch( Exception ex )
            {
                // Yeah, I don't like catching Exception, but unfortunately BaseNumberConverter (e.g. used for int) can *throw* a System.Exception (not a derived class) so there's nothing I can do about it.
                if( ex.InnerException is FormatException )
                    throw new CommandLineArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.ArgumentConversionErrorFormat, argument, Name, ArgumentType.Name), Name, ex);
                else
                    throw;
            }
        }

        /// <summary>
        /// Returns a <see cref="String"/> that represents the current <see cref="CommandLineArgument"/>.
        /// </summary>
        /// <returns>A <see cref="String"/> that represents the current <see cref="CommandLineArgument"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0} {1}", ArgumentType, Name);
        }
    }
}
