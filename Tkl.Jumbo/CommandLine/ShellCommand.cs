using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.ComponentModel;
using System.Globalization;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Represents a command that can be invoked through a command line application such as DfsShell or JetShell.
    /// </summary>
    /// <remarks>
    ///   Types that inherit from this class should specify the <see cref="ShellCommandAttribute"/>, and will be used
    ///   as an arguments class for <see cref="CommandLineParser"/>.
    /// </remarks>
    public abstract class ShellCommand
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ShellCommand"/> class.
        /// </summary>
        protected ShellCommand()
        {
        }

        /// <summary>
        /// Runs the command.
        /// </summary>
        public abstract void Run();

        /// <summary>
        /// Gets all shell command types in the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly whose types to search.</param>
        /// <returns>A list of types that inherit from <see cref="ShellCommand"/> and specify the <see cref="ShellCommandAttribute"/> attribute.</returns>
        public static Type[] GetShellCommands(Assembly assembly)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");

            return (from type in assembly.GetTypes()
                    where !type.IsAbstract && type.IsSubclassOf(typeof(ShellCommand)) && Attribute.IsDefined(type, typeof(ShellCommandAttribute))
                    select type).ToArray();
        }

        /// <summary>
        /// Prints a list of all commands in the specified assembly to the console.
        /// </summary>
        /// <param name="assembly">The assembly whose types to search.</param>
        public static void PrintAssemblyCommandList(Assembly assembly)
        {
            var commands = from command in GetShellCommands(assembly)
                           let name = ((ShellCommandAttribute)Attribute.GetCustomAttribute(command, typeof(ShellCommandAttribute))).CommandName
                           let description = ((DescriptionAttribute)Attribute.GetCustomAttribute(command, typeof(DescriptionAttribute))).Description
                           orderby name
                           select new { Name = name, Description = description };

            foreach( var command in commands )
            {
                Console.Write(string.Format(CultureInfo.CurrentCulture, "{0,13} : {1}", command.Name, command.Description).SplitLines(Console.WindowWidth - 1, 16));
                Console.WriteLine();
            }
        }

        /// <summary>
        /// Gets the shell command with the specified command name.
        /// </summary>
        /// <param name="assembly">The assembly whose types to search.</param>
        /// <param name="commandName">The command name of the shell command.</param>
        /// <returns>The <see cref="Type"/> of the specified shell command, or <see langword="null"/> if none could be found.</returns>
        public static Type GetShellCommand(Assembly assembly, string commandName)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");
            if( commandName == null )
                throw new ArgumentNullException("commandName");
            return (from type in assembly.GetTypes()
                    let attribute = (ShellCommandAttribute)Attribute.GetCustomAttribute(type, typeof(ShellCommandAttribute))
                    where type.IsSubclassOf(typeof(ShellCommand)) && attribute != null && string.Equals(attribute.CommandName, commandName, StringComparison.OrdinalIgnoreCase)
                    select type).SingleOrDefault();
        }
    }
}
