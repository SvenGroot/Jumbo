﻿using System;

namespace Tkl.Jumbo.CommandLine
{
    /// <summary>
    /// Indicates the constructor that should be used by the <see cref="CommandLineParser"/> class, if a class has multiple public constructors.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   If a class has only one public constructor, it is not necessary to use this attribute.
    /// </para>
    /// </remarks>
    /// <threadsafety static="true" instance="false"/>
    [AttributeUsage(AttributeTargets.Constructor)]
    public sealed class CommandLineConstructorAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineConstructorAttribute"/> class.
        /// </summary>
        public CommandLineConstructorAttribute()
        {
        }
    }
}