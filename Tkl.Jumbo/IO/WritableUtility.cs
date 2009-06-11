﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;

namespace Tkl.Jumbo.IO
{
    /// <summary>
    /// Provides utility functions for creating reflection-based <see cref="IWritable"/> implementations for classes.
    /// </summary>
    public static class WritableUtility
    {
        private static readonly Dictionary<Type, MethodInfo> _readMethods = CreateBinaryReaderMethodTable();

        /// <summary>
        /// Uses reflection to creates a function that serializes an object to a <see cref="BinaryWriter"/>; this function
        /// can be used in a <see cref="IWritable.Write"/> method.
        /// </summary>
        /// <typeparam name="T">The type of the object to serialize.</typeparam>
        /// <returns>A <see cref="Action{T, BinaryWriter}"/> delegate to a method that serializes the object.</returns>
        /// <remarks>
        /// <para>
        ///   The serializer created by this method will serialize only the public properties of the type which have
        ///   a public get and set method. If you need to serialize additional state, you should do that manually.
        /// </para>
        /// <para>
        ///   The serializer supports properties that have a type supported by one of the overloads of the <see cref="BinaryWriter.Write(string)"/>
        ///   method, as well those who implement <see cref="IWritable"/> themselves. The serializer supports
        ///   <see langword="null"/> values by writing a <see cref="Boolean"/> before each property that has a reference type
        ///   that indicates whether it's <see langword="null"/> or not.
        /// </para>
        /// </remarks>
        public static Action<T, BinaryWriter> CreateSerializer<T>()
        {
            Type type = typeof(T);
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            DynamicMethod serializer = new DynamicMethod("Write", null, new[] { type, typeof(BinaryWriter) }, type);
            ILGenerator generator = serializer.GetILGenerator();

            WriteArgNullCheck(generator, OpCodes.Ldarg_0, "obj");
            WriteArgNullCheck(generator, OpCodes.Ldarg_1, "writer");

            MethodInfo writeBooleanMethod = typeof(BinaryWriter).GetMethod("Write", new[] { typeof(bool) });

            foreach( PropertyInfo property in properties )
            {
                if( property.CanRead && property.CanWrite && !property.IsSpecialName )
                {
                    MethodInfo getMethod = property.GetGetMethod();
                    if( property.PropertyType.GetInterface(typeof(IWritable).FullName) != null )
                    {
                        generator.Emit(OpCodes.Ldarg_0); // Load the object.
                        generator.Emit(OpCodes.Callvirt, getMethod); // Get the property value.
                        Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, false, null);
                        generator.Emit(OpCodes.Ldarg_1); // load the writer.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Write", new[] { typeof(BinaryWriter) }));
                        if( endLabel != null )
                        {
                            generator.MarkLabel(endLabel.Value);
                        }
                    }
                    else if( property.PropertyType == typeof(DateTime) )
                    {
                        // For DateTimes we need to write the DateTimeKind and the ticks.
                        LocalBuilder dateLocal = generator.DeclareLocal(typeof(DateTime));
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack.
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value
                        generator.Emit(OpCodes.Stloc_S, dateLocal); // Store the date in a local
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack
                        generator.Emit(OpCodes.Ldloca_S, dateLocal); // put the address of the date on the stack (has to be the address for a property call to work)
                        generator.Emit(OpCodes.Call, typeof(DateTime).GetProperty("Kind").GetGetMethod()); // Get the DateTimeKind.
                        generator.Emit(OpCodes.Conv_U1); // Convert to a byte.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(byte) })); // Write the DateTimeKind to the stream.
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack
                        generator.Emit(OpCodes.Ldloca_S, dateLocal); // put the address of the date on the stack (has to be the address for a property call to work)
                        generator.Emit(OpCodes.Call, typeof(DateTime).GetProperty("Ticks").GetGetMethod()); // Get the Ticks.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(long) })); // write the ticks.
                    }
                    else if( property.PropertyType == typeof(byte[]) )
                    {
                        // We need to store the size and the data of the byte array.
                        LocalBuilder byteArrayLocal = generator.DeclareLocal(typeof(byte[]));
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // Get the property value.
                        generator.Emit(OpCodes.Stloc_S, byteArrayLocal); // store it in a local.
                        Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, false, byteArrayLocal);
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                        generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // put the byte array on the stack.
                        generator.Emit(OpCodes.Call, typeof(byte[]).GetProperty("Length").GetGetMethod()); // Get the length of the array.
                        generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Write7BitEncodedInt", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryWriter), typeof(int) }, null)); // Write length as compressed int.
                        generator.Emit(OpCodes.Ldarg_1); // put the writer on the stack.
                        generator.Emit(OpCodes.Ldloc_S, byteArrayLocal); // put the byte array on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryWriter).GetMethod("Write", new[] { typeof(byte[]) })); // Write the array data.
                        if( endLabel != null )
                        {
                            generator.MarkLabel(endLabel.Value);
                        }
                    }
                    else
                    {
                        MethodInfo writeMethod = typeof(BinaryWriter).GetMethod("Write", new[] { property.PropertyType });
                        if( writeMethod != null )
                        {
                            generator.Emit(OpCodes.Ldarg_1);
                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Callvirt, getMethod);
                            Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, true, null);
                            generator.Emit(OpCodes.Callvirt, writeMethod);
                            if( endLabel != null )
                            {
                                generator.MarkLabel(endLabel.Value);
                            }
                        }
                        else
                        {
                            throw new NotSupportedException(string.Format("Cannot generate an IWritable.Write implementation for type {0} because property {1} has unsupported type {2}.", typeof(T), property.Name, property.PropertyType));
                        }
                    }
                }
            }
            generator.Emit(OpCodes.Ret);

            serializer.DefineParameter(0, ParameterAttributes.In, "obj");
            serializer.DefineParameter(1, ParameterAttributes.In, "writer");

            return (Action<T, BinaryWriter>)serializer.CreateDelegate(typeof(Action<T, BinaryWriter>));
        }

        /// <summary>
        /// Uses reflection to create a function that deserializes an object from a <see cref="BinaryReader"/>; this function
        /// can be used in the object's <see cref="IWritable.Read"/> implementation.
        /// </summary>
        /// <typeparam name="T">The type of the object to deserialize.</typeparam>
        /// <returns>A <see cref="Action{T, BinaryReader}"/> delegate to a method that deserializes the object.</returns>
        /// <remarks>
        /// <para>
        ///   The function returned should only be used to deserialize data created by a function returned by <see cref="CreateSerializer{T}"/>.
        /// </para>
        /// </remarks>
        public static Action<T, BinaryReader> CreateDeserializer<T>()
        {
            Type type = typeof(T);

            DynamicMethod deserializer = new DynamicMethod("Read", null, new[] { type, typeof(BinaryReader) }, type);
            ILGenerator generator = deserializer.GetILGenerator();

            WriteArgNullCheck(generator, OpCodes.Ldarg_0, "obj");
            WriteArgNullCheck(generator, OpCodes.Ldarg_1, "reader");

            MethodInfo readBooleanMethod = typeof(BinaryReader).GetMethod("ReadBoolean");
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach( PropertyInfo property in properties )
            {
                if( property.CanWrite && property.CanRead && !property.IsSpecialName )
                {
                    Label? endLabel = null;
                    if( !property.PropertyType.IsValueType )
                    {
                        // Read a boolean to see if the property is null.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, readBooleanMethod);
                        Label nonNullLabel = generator.DefineLabel();
                        endLabel = generator.DefineLabel();
                        generator.Emit(OpCodes.Brtrue_S, nonNullLabel);
                        // False means that the value is true and was not written.
                        generator.Emit(OpCodes.Ldarg_0); // Load the object.
                        generator.Emit(OpCodes.Ldnull);
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true));
                        generator.Emit(OpCodes.Br_S, endLabel.Value);
                        generator.MarkLabel(nonNullLabel);
                    }

                    if( property.PropertyType.GetInterface(typeof(IWritable).FullName) != null )
                    {
                        LocalBuilder local = generator.DeclareLocal(property.PropertyType);
                        generator.Emit(OpCodes.Ldarg_0);// load the object.
                        generator.Emit(OpCodes.Callvirt, property.GetGetMethod()); // get the current property value.
                        generator.Emit(OpCodes.Stloc, local); // store it
                        generator.Emit(OpCodes.Ldloc, local); // load it.
                        Label nonNullLabel = generator.DefineLabel();
                        generator.Emit(OpCodes.Brtrue_S, nonNullLabel);
                        // Create a new instance if the object is null.
                        generator.Emit(OpCodes.Newobj, property.PropertyType.GetConstructor(Type.EmptyTypes));
                        generator.Emit(OpCodes.Stloc, local); // store it.
                        generator.Emit(OpCodes.Ldarg_0); // load the object.
                        generator.Emit(OpCodes.Ldloc, local); // load the new property object.
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // set the property value.
                        generator.MarkLabel(nonNullLabel);
                        generator.Emit(OpCodes.Ldloc, local); // load tjhe property value.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Read", new[] { typeof(BinaryReader) })); // Read the property from the reader.
                    }
                    else if( property.PropertyType == typeof(DateTime) )
                    {
                        LocalBuilder kindLocal = generator.DeclareLocal(typeof(DateTimeKind));
                        generator.Emit(OpCodes.Ldarg_0); // put the object ont the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadByte")); // read the DateTimeKind
                        generator.Emit(OpCodes.Conv_I4); // convert to int.
                        generator.Emit(OpCodes.Stloc_S, kindLocal); // store it.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadInt64")); // read the Ticks.
                        generator.Emit(OpCodes.Ldloc_S, kindLocal); // put the DateTimeKind on the stack.
                        generator.Emit(OpCodes.Newobj, typeof(DateTime).GetConstructor(new[] { typeof(long), typeof(DateTimeKind) })); // Create the DateTime instance.
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // Set the DateTime as the property value.
                    }
                    else if( property.PropertyType == typeof(byte[]) )
                    {
                        generator.Emit(OpCodes.Ldarg_0); // put the object on the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack.
                        generator.Emit(OpCodes.Ldarg_1); // put the reader on the stack (yes, twice).
                        generator.Emit(OpCodes.Call, typeof(WritableUtility).GetMethod("Read7BitEncodedInt", BindingFlags.Static | BindingFlags.Public, null, new[] { typeof(BinaryReader) }, null)); // read the length
                        generator.Emit(OpCodes.Callvirt, typeof(BinaryReader).GetMethod("ReadBytes", new[] { typeof(int) })); // read the byte array
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true)); // set the byte array as the property value.
                    }
                    else
                    {
                        MethodInfo readMethod = _readMethods[property.PropertyType];
                        generator.Emit(OpCodes.Ldarg_0); // load the object.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, readMethod);
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod(true));
                    }

                    if( endLabel != null )
                    {
                        generator.MarkLabel(endLabel.Value);
                    }
                }
            }

            generator.Emit(OpCodes.Ret);

            deserializer.DefineParameter(0, ParameterAttributes.In | ParameterAttributes.Out, "obj");
            deserializer.DefineParameter(1, ParameterAttributes.In, "reader");

            return (Action<T, BinaryReader>)deserializer.CreateDelegate(typeof(Action<T, BinaryReader>));
        }

        private static void WriteArgNullCheck(ILGenerator generator, OpCode ldArgOpCode, string argName)
        {
            Label label = generator.DefineLabel();
            generator.Emit(ldArgOpCode);
            generator.Emit(OpCodes.Brtrue_S, label);
            generator.Emit(OpCodes.Ldstr, argName);
            generator.Emit(OpCodes.Newobj, typeof(ArgumentNullException).GetConstructor(new[] { typeof(string) }));
            generator.Emit(OpCodes.Throw);
            generator.MarkLabel(label);
        }

        private static Label? WriteCheckForNullIfReferenceType(ILGenerator generator, MethodInfo writeBooleanMethod, PropertyInfo property, bool writerIsOnStack, LocalBuilder local)
        {
            Label? endLabel = null;
            if( !property.PropertyType.IsValueType )
            {
                if( local == null )
                {
                    local = generator.DeclareLocal(property.PropertyType);
                    generator.Emit(OpCodes.Stloc_S, local);
                    generator.Emit(OpCodes.Ldloc_S, local);
                }
                Label notNullLabel = generator.DefineLabel();
                endLabel = generator.DefineLabel();
                generator.Emit(OpCodes.Brtrue_S, notNullLabel);
                // This is code for if the value is null;
                if( !writerIsOnStack )
                    generator.Emit(OpCodes.Ldarg_1); // load the writer
                generator.Emit(OpCodes.Ldc_I4_0);
                generator.Emit(OpCodes.Callvirt, writeBooleanMethod);
                generator.Emit(OpCodes.Br_S, endLabel.Value);
                generator.MarkLabel(notNullLabel);
                // Code for if the value is not null
                if( !writerIsOnStack )
                    generator.Emit(OpCodes.Ldarg_1); // load the writer
                generator.Emit(OpCodes.Ldc_I4_1);
                generator.Emit(OpCodes.Callvirt, writeBooleanMethod);
                if( writerIsOnStack )
                    generator.Emit(OpCodes.Ldarg_1); // load the writer back on the stack.
                generator.Emit(OpCodes.Ldloc, local); // put the property value back on the stack
            }
            return endLabel;
        }

        private static Dictionary<Type, MethodInfo> CreateBinaryReaderMethodTable()
        {
            Dictionary<Type, MethodInfo> result = new Dictionary<Type, MethodInfo>();
            MethodInfo[] methods = typeof(BinaryReader).GetMethods(BindingFlags.Instance | BindingFlags.Public);
            foreach( MethodInfo method in methods )
            {
                if( method.Name.StartsWith("Read") && method.Name.Length > 4 && method.GetParameters().Length == 0 )
                {
                    result.Add(method.ReturnType, method);
                }
            }
            return result;
        }

        /// <summary>
        /// Writes a 32-bit integer in a compressed format.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the value to.</param>
        /// <param name="value">The 32-bit integer to be written.</param>
        public static void Write7BitEncodedInt(BinaryWriter writer, int value)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            uint uintValue = (uint)value; // this helps support negative numbers, not really needed but anyway.
            while( uintValue >= 0x80 )
            {
                writer.Write((byte)(uintValue | 0x80));
                uintValue = uintValue >> 7;
            }
            writer.Write((byte)uintValue);
        }

        /// <summary>
        /// Reads in a 32-bit integer in compressed format.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to read the value from.</param>
        /// <returns>A 32-bit integer in compressed format. </returns>
        public static int Read7BitEncodedInt(BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            byte currentByte;
            int result = 0;
            int bits = 0;
            do
            {
                if( bits == 35 )
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = reader.ReadByte();
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while( (currentByte & 0x80) != 0 );
            return result;
        }
    }
}
