// $Id$
//
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Parses command line arguments into a class of the specified type.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The command line arguments that are accepted by the parser are determined by the type passed to the constructor.
    /// </para>
    /// <para>
    ///   The <see cref="CommandLineParser"/> class distinguishes two types of command line arguments: positional and named arguments.
    /// </para>
    /// <para>
    ///   Positional arguments are identified by the order in which they appear on the command line. For instance, if you invoke the Windows
    ///   copy command with "copy file.ext c:\", both parameters are positional arguments, and their meaning is determined by the order.
    ///   "file.ext" is the source, because it is the first positional argument, while "c:\" is the destination, because it is the
    ///   second positional argument.
    /// </para>
    /// <para>
    ///   Positional arguments can be optional. An optional argument can be omitted from the command line, in which case it will have
    ///   its default value. Note that you cannot have any required positional arguments following an optional argument, because in that case
    ///   it is not possible to determine if the argument has been omitted.
    /// </para>
    /// <para>
    ///   Named arguments are identified by name, and are preceded by a special character to distinguish them from the positional arguments.
    ///   On Windows, this character is typically a forward slash "/", while on Unix it's typically a dash "-". You can specify which
    ///   character to use by setting the <see cref="NamedArgumentSwitch"/> property. This property will default to "/" on Windows and
    ///   to "-" on Unix operating systems (Unix is supported via <a href="http://www.mono-project.com">Mono</a>).
    /// </para>
    /// <para>
    ///   Named arguments can appear in any order, and are never required. A named argument can have a value which is specified
    ///   after a colon following the attribute name, e.g. "/argument:value". Alternatively, a named attribute can simply have
    ///   a meaning defined by its presence or absence. For example, many applications use /v to indicate you want verbose output,
    ///   while the absence of that argument means you don't want verbose output. These kinds of arguments are created by the
    ///   <see cref="CommandLineParser"/> by using the <see cref="Boolean"/> type for the argument.
    /// </para>
    /// <para>
    ///   The parameters of the type's constructor will be used as the positional arguments. If the
    ///   type has more than one constructor, the constructor that has the <see cref="CommandLineConstructorAttribute"/> attribute
    ///   applied will be used (to have multiple constructors but none marked with this attribute or multiple constructors with this
    ///   attribute is an error).
    /// </para>
    /// <para>
    ///   To create an optional positional argument, apply the <see cref="System.Runtime.InteropServices.OptionalAttribute"/> attribute to the constructor parameter.
    ///   To set the default value, apply the <see cref="System.Runtime.InteropServices.DefaultParameterValueAttribute"/> to the constructor parameter.
    ///   In Visual Basic, you can use the <see langword="Optional"/> keyword and built-in syntax to specify the default value.
    /// </para>
    /// <para>
    ///   Properties of the type that have the <see cref="NamedCommandLineArgumentAttribute"/> attribute applied will be used as named
    ///   command line arguments for the type.
    /// </para>
    /// <para>
    ///   Arrays are supported for both named and positional arguments. For positional arguments, only the last argument may be an array, in which case all remaining positional
    ///   arguments specified on the command line will be elements of this array. For named arguments with an array type, you can repeat the argument multiple times, e.g. "Program.exe /val:foo /val:bar"
    ///   will set the "val" argument to an array containing { "foo", "bar" } if it's an array argument.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false"/>
    public class CommandLineParser
    {
        private const char _nameValueSeparator = ':';

        private readonly Type _argumentsType;
        private readonly PositionalCommandLineArgument[] _positionalArguments;
        private readonly SortedList<string, NamedCommandLineArgument> _namedArguments = new SortedList<string, NamedCommandLineArgument>();
        private readonly int _minimumArgumentCount;
        private readonly ConstructorInfo _commandLineConstructor;
        private string _namedArgumentSwitch;
        private ReadOnlyCollection<PositionalCommandLineArgument> _positionalArgumentsReadOnlyWrapper;
        private ReadOnlyCollection<NamedCommandLineArgument> _namedArgumentsReadOnlyWrapper;

        /// <summary>
        /// Event raised when an argument is parsed from the command line.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If the event handler sets the <see cref="CancelEventArgs.Cancel"/> property to <see langword="true"/>, command line processing will stop immediately,
        ///   and the <see cref="CommandLineParser.Parse"/> method will return <see langword="null"/>, even if all the required positional parameters have already
        ///   been parsed. You can use this for instance to implement a "/?" argument that will display usage and quit regardless of the other command line arguments.
        /// </para>
        /// </remarks>        
        public event EventHandler<ArgumentParsedEventArgs> ArgumentParsed;

        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineParser"/> class.
        /// </summary>
        /// <param name="argumentsType">The type of the class holding the command line arguments.</param>
        /// <exception cref="NotSupportedException">The <see cref="CommandLineParser"/> cannot use <paramref name="argumentsType"/> as the command line arguments type.</exception>
        public CommandLineParser(Type argumentsType)
        {
            if( argumentsType == null )
                throw new ArgumentNullException("argumentsType");

            _argumentsType = argumentsType;
            _commandLineConstructor = GetCommandLineConstructor();

            ParameterInfo[] parameters = _commandLineConstructor.GetParameters();
            _positionalArguments = new PositionalCommandLineArgument[parameters.Length];
            bool hasOptionalAttribute = false;
            for( int x = 0; x < parameters.Length; ++x )
            {
                ParameterInfo parameter = parameters[x];

                if( !parameter.IsOptional && hasOptionalAttribute )
                    throw new NotSupportedException(Properties.Resources.InvalidOptionalArgumentOrder);
                else if( parameter.IsOptional && !hasOptionalAttribute )
                {
                    hasOptionalAttribute = true;
                    _minimumArgumentCount = x;
                }

                if( parameter.ParameterType.IsArray && x != parameters.Length - 1 )
                    throw new NotSupportedException(Properties.Resources.ArrayNotLastArgument);

                _positionalArguments[x] = new PositionalCommandLineArgument(parameter);
            }

            if( !hasOptionalAttribute )
                _minimumArgumentCount = _positionalArguments.Length;

            PropertyInfo[] properties = _argumentsType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach( PropertyInfo prop in properties )
            {
                if( Attribute.IsDefined(prop, typeof(NamedCommandLineArgumentAttribute)) )
                {
                    NamedCommandLineArgument argument = new NamedCommandLineArgument(prop);
                    if( argument.Name.Contains(_nameValueSeparator) )
                        throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.NamedArgumentContainsSeparatorFormat, argument.Name));
                    _namedArguments.Add(argument.Name, argument);
                }
            }

            NamedArgumentSwitch = (Environment.OSVersion.Platform == PlatformID.Unix || Environment.OSVersion.Platform == PlatformID.MacOSX) ? "-" : "/";
        }

        /// <summary>
        /// Gets or sets the switch character for named arguments.
        /// </summary>
        /// <value>The switch character for named arguments. The default value is '/' on Windows, and '-' on Unix.</value>
        /// <remarks>
        /// <para>
        ///   The named argument switch may not contain a colon, because that is the separator for argument names and values for named arguments.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentException">The new property value is empty or contains a colon (:).</exception>
        public string NamedArgumentSwitch
        {
            get { return _namedArgumentSwitch; }
            set 
            {
                if( string.IsNullOrEmpty(value) )
                    throw new ArgumentException(Properties.Resources.EmptyNamedArgumentSwitch, "value");
                if( value.Contains(_nameValueSeparator) )
                    throw new ArgumentException(Properties.Resources.NamedArgumentSwitchContainsSeparator, "value");
                _namedArgumentSwitch = value;
            }
        }

        /// <summary>
        /// Gets a description of the command line application.
        /// </summary>
        /// <value>
        /// The description of the command line application. The default value is an empty string ("").
        /// </value>
        /// <remarks>
        /// <para>
        ///   This description will be added to the usage returned by the <see cref="Usage"/> property. This description can be set by applying
        ///   the <see cref="DescriptionAttribute"/> to the command line arguments type.
        /// </para>
        /// </remarks>
        public string Description
        {
            get
            {
                DescriptionAttribute description = (DescriptionAttribute)Attribute.GetCustomAttribute(_argumentsType, typeof(DescriptionAttribute));
                return description == null ? "" : description.Description;
            }
        }

        /// <summary>
        /// Gets the positional arguments supported by this <see cref="CommandLineParser"/> instance.
        /// </summary>
        /// <value>
        /// A list of the positional arguments.
        /// </value>
        /// <remarks>
        /// <para>
        ///   The value of this property can be used for informational purposes, but it cannot be used to retrieve the values of the arguments after a parse operation.
        /// </para>
        /// </remarks>
        public ReadOnlyCollection<PositionalCommandLineArgument> PositionalArguments
        {
            get
            {
                return _positionalArgumentsReadOnlyWrapper ?? (_positionalArgumentsReadOnlyWrapper = new ReadOnlyCollection<PositionalCommandLineArgument>(_positionalArguments));
            }
        }

        /// <summary>
        /// Gets the named arguments supported by this <see cref="CommandLineParser"/> instance.
        /// </summary>
        /// <value>
        /// A list of the named arguments.
        /// </value>
        /// <remarks>
        /// <para>
        ///   The value of this property can be used for informational purposes, but it cannot be used to retrieve the values of the arguments after a parse operation.
        /// </para>
        /// </remarks>
        public ReadOnlyCollection<NamedCommandLineArgument> NamedArguments
        {
            get
            {
                return _namedArgumentsReadOnlyWrapper ?? (_namedArgumentsReadOnlyWrapper = new ReadOnlyCollection<NamedCommandLineArgument>(_namedArguments.Values));
            }
        }

        /// <summary>
        /// Gets usage information for the command line application, using the file name of the entry point assembly as the executable name, and
        /// <see cref="Console.WindowWidth"/> - 1 as the line width.
        /// </summary>
        /// <value>
        /// Usage information for the command line application.
        /// </value>
        /// <remarks>
        /// <para>
        ///   You can add descriptions to the usage text by applying the <see cref="DescriptionAttribute"/> attribute to your command line arguments class,
        ///   the constructor parameters for positional arguments, and the properties for named arguments.
        /// </para>
        /// <para>
        ///   To customize the executable name and the line width, use <see cref="GetCustomUsage(string,int)"/> instead.
        /// </para>
        /// </remarks>
        public string Usage
        {
            get
            {
                string usagePrefix = string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.DefaultUsagePrefixFormat, System.IO.Path.GetFileName(Assembly.GetEntryAssembly().Location));
                return GetCustomUsage(usagePrefix, Console.WindowWidth - 1);
            }
        }

        /// <summary>
        /// Gets a string describing the command line usage of the job runner.
        /// </summary>
        /// <param name="usagePrefix">A string to prepend to the first line of the usage text (e.g. the executable name).</param>
        /// <param name="maxLineLength">The maximum line length of lines in the usage text.</param>
        /// <returns>A string describing the command line usage of the job runner.</returns>
        /// <remarks>
        /// <para>
        ///   You can add descriptions to the usage text by applying the <see cref="DescriptionAttribute"/> attribute to your command line arguments class,
        ///   the constructor parameters for positional arguments, and the properties for named arguments.
        /// </para>
        /// <para>
        ///   When using this function to get usage text for display on the console, use <see cref="Console.WindowWidth"/> - 1
        ///   as the value for <paramref name="maxLineLength"/>. If you don't subtract 1, this can lead to blank lines in
        ///   case a line is exactly the maximum width.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="usagePrefix"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxLineLength"/> is not greater than zero.</exception>
        public string GetCustomUsage(string usagePrefix, int maxLineLength)
        {
            return GetCustomUsage(usagePrefix, maxLineLength, Properties.Resources.DefaultRequiredArgumentFormat, Properties.Resources.DefaultOptionalArgumentFormat, Properties.Resources.DefaultOptionalArgumentWithDefaultValueFormat, Properties.Resources.DefaultArraySuffix, Properties.Resources.DefaultArgumentDescriptionFormat, 16);
        }

        /// <summary>
        /// Gets a string describing the command line usage of the job runner using custom argument formatting.
        /// </summary>
        /// <param name="usagePrefix">A string to prepend to the first line of the usage text (e.g. the executable name).</param>
        /// <param name="maxLineLength">The maximum line length of lines in the usage text.</param>
        /// <param name="requiredArgumentFormat">The format string to use for required positional arguments, e.g. "&lt;{0}&gt;".</param>
        /// <param name="optionalArgumentFormat">The format string to use for optional arguments (positional and named), e.g. "[{0}]".</param>
        /// <param name="optionalArgumentWithDefaultValueFormat">The format string to use for optional positional arguments that have a default value, e.g. "[{0}={1}]".
        /// The default value for named arguments is not included in the usage.</param>
        /// <param name="arraySuffix">The text to append to the name of an array argument, e.g. "...".</param>
        /// <param name="argumentDescriptionFormat">The format string to use for the description of an argument, e.g. "{0,13} : {1}".</param>
        /// <param name="argumentDescriptionIndent">The amount of characters by which to indent the argument descriptions (after the first line of each argument).</param>
        /// <returns>A string describing the command line usage of the job runner.</returns>
        /// <remarks>
        /// <para>
        ///   You can add descriptions to the usage text by applying the <see cref="DescriptionAttribute"/> attribute to your command line arguments class,
        ///   the constructor parameters for positional arguments, and the properties for named arguments.
        /// </para>
        /// <para>
        ///   When using this function to get usage text for display on the console, use <see cref="Console.WindowWidth"/> - 1
        ///   as the value for <paramref name="maxLineLength"/>. If you don't subtract 1, this can lead to blank lines in
        ///   case a line is exactly the maximum width.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="usagePrefix"/>, <paramref name="requiredArgumentFormat"/>, <paramref name="optionalArgumentFormat"/>,
        /// <paramref name="optionalArgumentWithDefaultValueFormat"/>, <paramref name="arraySuffix"/> or <paramref name="argumentDescriptionFormat"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxLineLength"/> is not greater than zero.</exception>
        public string GetCustomUsage(string usagePrefix, int maxLineLength, string requiredArgumentFormat, string optionalArgumentFormat, string optionalArgumentWithDefaultValueFormat, string arraySuffix, string argumentDescriptionFormat, int argumentDescriptionIndent)
        {
            if( usagePrefix == null )
                throw new ArgumentNullException("usagePrefix");
            if( maxLineLength <= 0 )
                throw new ArgumentOutOfRangeException("maxLineLength", Properties.Resources.MaxLineLengthOutOfRange);
            if( requiredArgumentFormat == null )
                throw new ArgumentNullException("requiredArgumentFormat");
            if( optionalArgumentFormat == null )
                throw new ArgumentNullException("optionalArgumentFormat");
            if( optionalArgumentWithDefaultValueFormat == null )
                throw new ArgumentNullException("optionalArgumentWithDefaultValueFormat");
            if( arraySuffix == null )
                throw new ArgumentNullException("arraySuffix");
            if( argumentDescriptionFormat == null )
                throw new ArgumentNullException("argumentDescriptionFormat");

            StringBuilder argumentBuilder = new StringBuilder();
            StringBuilder argumentUsage = new StringBuilder();
            argumentUsage.Append(usagePrefix);
            foreach( NamedCommandLineArgument argument in _namedArguments.Values )
            {
                argumentUsage.Append(" ");
                argumentBuilder.Length = 0;
                argumentBuilder.Append(NamedArgumentSwitch);
                argumentBuilder.Append(argument.Name);
                if( !(argument.ArgumentType == typeof(bool) || (argument.ArgumentType.IsArray && argument.ArgumentType.GetElementType() == typeof(bool))) )
                {
                    argumentBuilder.Append(_nameValueSeparator);
                    argumentBuilder.Append(argument.PropertyName);
                }
                if( argument.ArgumentType.IsArray )
                    argumentBuilder.Append(arraySuffix);
                argumentUsage.AppendFormat(System.Globalization.CultureInfo.CurrentCulture, optionalArgumentFormat, argumentBuilder.ToString());
            }

            foreach( PositionalCommandLineArgument argument in _positionalArguments )
            {
                argumentUsage.Append(" ");
                string argumentName = argument.Name;
                if( argument.ArgumentType.IsArray )
                    argumentName += arraySuffix;
                if( argument.IsOptional )
                {
                    if( argument.DefaultValue != null )
                        argumentUsage.AppendFormat(System.Globalization.CultureInfo.CurrentCulture, optionalArgumentWithDefaultValueFormat, argumentName, argument.DefaultValue);
                    else
                        argumentUsage.AppendFormat(System.Globalization.CultureInfo.CurrentCulture, optionalArgumentFormat, argumentName);
                }
                else
                    argumentUsage.AppendFormat(System.Globalization.CultureInfo.CurrentCulture, requiredArgumentFormat, argumentName); ;
            }

            StringBuilder usage = new StringBuilder();
            if( !string.IsNullOrEmpty(Description) )
            {
                usage.Insert(0, Description.SplitLines(maxLineLength, 0) + Environment.NewLine);
            }

            usage.Append(argumentUsage.ToString().SplitLines(maxLineLength, 3));

            foreach( PositionalCommandLineArgument argument in _positionalArguments )
            {
                if( !string.IsNullOrEmpty(argument.Description) )
                {
                    usage.AppendLine();
                    usage.Append(string.Format(System.Globalization.CultureInfo.CurrentCulture, argumentDescriptionFormat, argument.Name, argument.Description).SplitLines(maxLineLength, argumentDescriptionIndent));
                }
            }

            foreach( NamedCommandLineArgument argument in _namedArguments.Values )
            {
                usage.AppendLine();
                usage.Append(string.Format(System.Globalization.CultureInfo.CurrentCulture, argumentDescriptionFormat, NamedArgumentSwitch + argument.Name, argument.Description).SplitLines(maxLineLength, argumentDescriptionIndent));
            }

            return usage.ToString();
        }

        /// <summary>
        /// Parses the specified command line arguments and creates an instance of the command line arguments type containing the arguments.
        /// </summary>
        /// <param name="args">The command line arguments.</param>
        /// <param name="index">The index of the first argument to parse.</param>
        /// <returns>An instance of the command line arguments type, or <see langword="null"/> if there are too many or too few positional arguments.</returns>
        /// <exception cref="CommandLineArgumentException">An unknown named argument was used, or a named argument was missing a value,
        /// or one of the argument values could not be converted to the argument type.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="args"/> is <see langword="null"/>.</exception>
        public object Parse(string[] args, int index)
        {
            if( args == null )
                throw new ArgumentNullException("args");
            if( index < 0 || index > args.Length )
                throw new ArgumentOutOfRangeException("index");

            // Reset all arguments to their default value.
            foreach( NamedCommandLineArgument argument in _namedArguments.Values )
                argument.Value = argument.DefaultValue;
            foreach( PositionalCommandLineArgument argument in _positionalArguments )
                argument.Value = argument.DefaultValue;

            int positionalArgumentIndex = 0;

            for( int x = index; x < args.Length; ++x )
            {
                string arg = args[x];
                bool cancel;
                if( arg.StartsWith(NamedArgumentSwitch, StringComparison.Ordinal) )
                {
                    cancel = ParseNamedArgument(arg);
                }
                else
                {
                    if( positionalArgumentIndex >= _positionalArguments.Length )
                        return null;
                    cancel = ParsePositionalArgument(positionalArgumentIndex, arg);
                    if( !_positionalArguments[positionalArgumentIndex].ArgumentType.IsArray )
                        ++positionalArgumentIndex;
                }
                if( cancel )
                    return null;
            }

            if( positionalArgumentIndex < _positionalArguments.Length && _positionalArguments[positionalArgumentIndex].ArgumentType.IsArray && _positionalArguments[positionalArgumentIndex].Value != null )
                ++positionalArgumentIndex;

            if( positionalArgumentIndex < _minimumArgumentCount )
                return null;

            if( _positionalArguments.Length > 0 )
            {
                PositionalCommandLineArgument lastArgument = _positionalArguments[_positionalArguments.Length - 1];
                if( lastArgument.ArgumentType.IsArray )
                {
                    List<object> items = (List<object>)_positionalArguments[_positionalArguments.Length - 1].Value;
                    if( items != null )
                        _positionalArguments[_positionalArguments.Length - 1].Value = ConvertToArray(lastArgument.ArgumentType.GetElementType(), items);
                }
            }

            object commandLineArguments = Activator.CreateInstance(_argumentsType, (from arg in _positionalArguments select arg.Value).ToArray());
            foreach( NamedCommandLineArgument argument in _namedArguments.Values )
            {
                if( argument.ArgumentType.IsArray )
                {
                    List<object> items = (List<object>)argument.Value;
                    if( items != null )
                        argument.Value = ConvertToArray(argument.ArgumentType.GetElementType(), items);
                }
                argument.ApplyValue(commandLineArguments);
            }
            return commandLineArguments;
        }

        /// <summary>
        /// Raises the <see cref="ArgumentParsed"/> event.
        /// </summary>
        /// <param name="e">The data for the event.</param>
        protected virtual void OnArgumentParsed(ArgumentParsedEventArgs e)
        {
            EventHandler<ArgumentParsedEventArgs> handler = ArgumentParsed;
            if( handler != null )
                handler(this, e);
        }

        private bool ParsePositionalArgument(int positionalArgumentIndex, string arg)
        {
            PositionalCommandLineArgument argument = _positionalArguments[positionalArgumentIndex];

            object value = argument.ConvertToArgumentType(arg);
            if( argument.ArgumentType.IsArray )
            {
                if( argument.Value == null )
                    argument.Value = new List<object>();
                ((List<object>)argument.Value).Add(value);
            }
            else
            {
                argument.Value = value;
            }
            ArgumentParsedEventArgs e = new ArgumentParsedEventArgs(argument, value);
            OnArgumentParsed(e);
            return e.Cancel;
        }

        private bool ParseNamedArgument(string arg)
        {
            string argumentName = null;
            string argumentValue = null;

            // We don't use Split because if there's more than one colon we want to ignore the others.
            int colonIndex = arg.IndexOf(_nameValueSeparator);
            if( colonIndex >= 0 )
            {
                argumentName = arg.Substring(NamedArgumentSwitch.Length, colonIndex - NamedArgumentSwitch.Length);
                argumentValue = arg.Substring(colonIndex + 1);
            }
            else
                argumentName = arg.Substring(NamedArgumentSwitch.Length);

            NamedCommandLineArgument argument;
            if( _namedArguments.TryGetValue(argumentName, out argument) )
            {
                object convertedValue;
                if( (argument.ArgumentType == typeof(bool) || (argument.ArgumentType.IsArray && argument.ArgumentType.GetElementType() == typeof(bool))) && argumentValue == null )
                {
                    convertedValue = true;
                    argument.Value = true;
                }
                else
                {
                    if( argumentValue != null )
                    {
                        convertedValue = argument.ConvertToArgumentType(argumentValue);
                        if( argument.ArgumentType.IsArray )
                        {
                            if( argument.Value == null )
                                argument.Value = new List<object>();
                            ((List<object>)argument.Value).Add(convertedValue);
                        }
                        else
                            argument.Value = convertedValue;
                    }
                    else
                        throw new CommandLineArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.MissingArgumentValueFormat, argument.Name), argument.Name);
                }

                ArgumentParsedEventArgs e = new ArgumentParsedEventArgs(argument, convertedValue);
                OnArgumentParsed(e);
                return e.Cancel;
            }
            else
                throw new CommandLineArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, Properties.Resources.UnknownArgumentFormat, argumentName), argumentName);
        }

        private ConstructorInfo GetCommandLineConstructor()
        {
            ConstructorInfo[] ctors = _argumentsType.GetConstructors();
            ConstructorInfo ctor;
            if( ctors.Length < 1 )
                throw new NotSupportedException(Properties.Resources.NoConstructor);
            else if( ctors.Length > 1 )
            {
                try
                {
                    ctor = (from c in ctors
                            where Attribute.IsDefined(c, typeof(CommandLineConstructorAttribute))
                            select c).SingleOrDefault();

                    if( ctor == null )
                        throw new NotSupportedException(Properties.Resources.NoMarkedConstructor);
                }
                catch( InvalidOperationException ex )
                {
                    throw new NotSupportedException(Properties.Resources.MultipleMarkedConstructors, ex);
                }
            }
            else // ctors.Length == 1
                ctor = ctors[0];
            return ctor;
        }

        private static Array ConvertToArray(Type elementType, List<object> items)
        {
            Array result = Array.CreateInstance(elementType, items.Count);
            for( int x = 0; x < items.Count; ++x )
                result.SetValue(items[x], x);
            return result;
        }
    }
}
