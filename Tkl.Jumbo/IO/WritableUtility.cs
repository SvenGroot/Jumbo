using System;
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
                        Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, false);
                        generator.Emit(OpCodes.Ldarg_1); // load the writer.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Write", new[] { typeof(BinaryWriter) }));
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
                            // TODO: Maybe special case byte array.
                            generator.Emit(OpCodes.Ldarg_1);
                            generator.Emit(OpCodes.Ldarg_0);
                            generator.Emit(OpCodes.Callvirt, getMethod);
                            Label? endLabel = WriteCheckForNullIfReferenceType(generator, writeBooleanMethod, property, true);
                            generator.Emit(OpCodes.Callvirt, writeMethod);
                            if( endLabel != null )
                            {
                                generator.MarkLabel(endLabel.Value);
                            }
                        }
                        else
                        {
                            throw new NotSupportedException(string.Format("Cannot generate an IWritable.Write implementation for type {0} because property {1} has unsupported type {2}.", typeof(T), property, property.PropertyType));
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
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod());
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
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod()); // set the property value.
                        generator.MarkLabel(nonNullLabel);
                        generator.Emit(OpCodes.Ldloc, local); // load tjhe property value.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, typeof(IWritable).GetMethod("Read", new[] { typeof(BinaryReader) })); // Read the property from the reader.
                    }
                    else
                    {
                        // TODO: Maye a special case for byte arrays.
                        MethodInfo readMethod = _readMethods[property.PropertyType];
                        generator.Emit(OpCodes.Ldarg_0); // load the object.
                        generator.Emit(OpCodes.Ldarg_1); // load the reader.
                        generator.Emit(OpCodes.Callvirt, readMethod);
                        generator.Emit(OpCodes.Callvirt, property.GetSetMethod());
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

        private static Label? WriteCheckForNullIfReferenceType(ILGenerator generator, MethodInfo writeBooleanMethod, PropertyInfo property, bool writerIsOnStack)
        {
            Label? endLabel = null;
            if( !property.PropertyType.IsValueType )
            {
                LocalBuilder local = generator.DeclareLocal(property.PropertyType);
                generator.Emit(OpCodes.Stloc, local);
                generator.Emit(OpCodes.Ldloc, local);
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
    }
}
