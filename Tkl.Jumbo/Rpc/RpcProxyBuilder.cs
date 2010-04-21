// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection.Emit;
using System.Reflection;

namespace Tkl.Jumbo.Rpc
{
    static class RpcProxyBuilder
    {
        private static readonly AssemblyBuilder _proxyAssembly;
        private static readonly ModuleBuilder _proxyModule;
        private static readonly Dictionary<Type, Type> _proxies = new Dictionary<Type, Type>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static RpcProxyBuilder()
        {
            _proxyAssembly = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName("Tkl.Jumbo.Rpc.DynamicProxy"), AssemblyBuilderAccess.Run);
            _proxyModule = _proxyAssembly.DefineDynamicModule("DynamicProxyModule");
        }

        public static object GetProxy(Type interfaceType, string hostName, int port, string objectName)
        {
            Type proxyType;
            lock( _proxies )
            {
                if( !_proxies.TryGetValue(interfaceType, out proxyType) )
                {
                    proxyType = CreateProxy(interfaceType);
                    _proxies.Add(interfaceType, proxyType);
                }
            }

            return Activator.CreateInstance(proxyType, hostName, port, objectName);
        }

        // Called inside _proxies lock for thread safety.
        private static Type CreateProxy(Type interfaceType)
        {
            if( interfaceType == null )
                throw new ArgumentNullException("interfaceType");
            if( !interfaceType.IsInterface )
                throw new ArgumentException("Type is not an interface.", "interfaceType");
            if( interfaceType.IsGenericType || interfaceType.IsGenericTypeDefinition )
                throw new ArgumentException("Generic types are not supported.");

            TypeBuilder proxyType = _proxyModule.DefineType("Tkl.Jumbo.Rpc.DynamicProxy." + interfaceType.FullName.Replace('.', '_').Replace('+', '_'), TypeAttributes.Class | TypeAttributes.Sealed | TypeAttributes.Public | TypeAttributes.BeforeFieldInit, typeof(object), new[] { interfaceType });
            FieldBuilder hostNameField = proxyType.DefineField("_hostName", typeof(string), FieldAttributes.Private | FieldAttributes.InitOnly);
            FieldBuilder portField = proxyType.DefineField("_port", typeof(int), FieldAttributes.Private | FieldAttributes.InitOnly);
            FieldBuilder objectNameField = proxyType.DefineField("_objectName", typeof(string), FieldAttributes.Private | FieldAttributes.InitOnly);

            CreateConstructor(proxyType, hostNameField, portField, objectNameField);

            foreach( MemberInfo member in interfaceType.GetMembers() )
            {
                switch( member.MemberType )
                {
                case MemberTypes.Method:
                    CreateMethod(interfaceType, (MethodInfo)member, proxyType, hostNameField, portField, objectNameField);
                    break;
                case MemberTypes.Property:
                    CreateProperty(interfaceType, (PropertyInfo)member, proxyType, hostNameField, portField, objectNameField);
                    break;
                default:
                    throw new NotSupportedException("Interface has unsupported member type.");
                }
            }

            return proxyType.CreateType();
        }

        private static MethodBuilder CreateMethod(Type interfaceType, MethodInfo interfaceMethod, TypeBuilder proxyType, FieldBuilder hostNameField, FieldBuilder portField, FieldBuilder objectNameField)
        {
            if( interfaceMethod.IsGenericMethod || interfaceMethod.IsGenericMethodDefinition )
                throw new NotSupportedException("Generic methods are not supported.");

            ParameterInfo[] parameters = interfaceMethod.GetParameters();
            MethodAttributes attributes = interfaceMethod.Attributes & ~(MethodAttributes.Abstract) | MethodAttributes.Virtual | MethodAttributes.Final;
            MethodBuilder proxyMethod = proxyType.DefineMethod(interfaceMethod.Name, attributes, interfaceMethod.CallingConvention, interfaceMethod.ReturnType, parameters.Select(p => p.ParameterType).ToArray());
            foreach( ParameterInfo param in parameters )
            {
                if( param.ParameterType.IsByRef )
                    throw new NotSupportedException("Interface methods with reference parameters are not supported.");
                proxyMethod.DefineParameter(param.Position + 1, param.Attributes, param.Name);
            }

            ILGenerator generator = proxyMethod.GetILGenerator();
            generator.DeclareLocal(typeof(object[]));
            generator.Emit(OpCodes.Ldarg_0); // Load this
            generator.Emit(OpCodes.Ldfld, hostNameField); // Load the host name field
            generator.Emit(OpCodes.Ldarg_0); // Load this
            generator.Emit(OpCodes.Ldfld, portField); // Load the port field
            generator.Emit(OpCodes.Ldarg_0); // Load this
            generator.Emit(OpCodes.Ldfld, objectNameField); // Load the object name field
            generator.Emit(OpCodes.Ldstr, interfaceType.AssemblyQualifiedName); // Load the interface name
            generator.Emit(OpCodes.Ldstr, interfaceMethod.Name); // Load the method name
            if( parameters.Length == 0 )
                generator.Emit(OpCodes.Ldnull);
            else
            {
                generator.Emit(OpCodes.Ldc_I4, parameters.Length); // Load the number of parameters
                generator.Emit(OpCodes.Newarr, typeof(object)); // Create a new array
                generator.Emit(OpCodes.Stloc_0); // Store the array
                for( int x = 0; x < parameters.Length; ++x )
                {
                    generator.Emit(OpCodes.Ldloc_0); // Load the array
                    generator.Emit(OpCodes.Ldc_I4, x); // Load the index
                    generator.Emit(OpCodes.Ldarg_S, x + 1); // Load the argument
                    if( parameters[x].ParameterType.IsValueType )
                        generator.Emit(OpCodes.Box, parameters[x].ParameterType); // Box the argument if it's a value type
                    generator.Emit(OpCodes.Stelem_Ref); // Store the argument in the array at the specified index.
                }

                generator.Emit(OpCodes.Ldloc_0); // Load the array
            }
            MethodInfo sendRequestMethod = typeof(RpcClient).GetMethod("SendRequest");
            generator.Emit(OpCodes.Call, sendRequestMethod); // Call the SendRequest method

            if( interfaceMethod.ReturnType == typeof(void) )
                generator.Emit(OpCodes.Pop); // Pop the return value off the stack
            else if( interfaceMethod.ReturnType.IsValueType )
                generator.Emit(OpCodes.Unbox_Any, interfaceMethod.ReturnType); // Unbox the return type
            else if( interfaceMethod.ReturnType != typeof(object) )
                generator.Emit(OpCodes.Castclass, interfaceMethod.ReturnType); // Cast the return type

            generator.Emit(OpCodes.Ret);

            return proxyMethod;
        }

        private static void CreateProperty(Type interfaceType, PropertyInfo interfaceProperty, TypeBuilder proxyType, FieldBuilder hostNameField, FieldBuilder portField, FieldBuilder objectNameField)
        {
            ParameterInfo[] indexParameters = interfaceProperty.GetIndexParameters();
            PropertyBuilder proxyProperty = proxyType.DefineProperty(interfaceProperty.Name, interfaceProperty.Attributes, interfaceProperty.PropertyType, indexParameters.Select(p => p.ParameterType).ToArray());

            if( interfaceProperty.CanRead )
            {
                MethodBuilder getMethod = CreateMethod(interfaceType, interfaceProperty.GetGetMethod(), proxyType, hostNameField, portField, objectNameField);
                proxyProperty.SetGetMethod(getMethod);
            }

            if( interfaceProperty.CanWrite )
            {
                MethodBuilder setMethod = CreateMethod(interfaceType, interfaceProperty.GetSetMethod(), proxyType, hostNameField, portField, objectNameField);
                proxyProperty.SetSetMethod(setMethod);
            }
        }

        private static void CreateConstructor(TypeBuilder proxyType, FieldBuilder hostNameField, FieldBuilder portField, FieldBuilder objectNameField)
        {
            ConstructorBuilder ctor = proxyType.DefineConstructor(MethodAttributes.Public, CallingConventions.HasThis, new[] { typeof(string), typeof(int), typeof(string) });
            ctor.DefineParameter(1, ParameterAttributes.In, "hostName");
            ctor.DefineParameter(2, ParameterAttributes.In, "port");
            ctor.DefineParameter(3, ParameterAttributes.In, "objectName");

            ILGenerator generator = ctor.GetILGenerator();
            generator.Emit(OpCodes.Ldarg_0); // Load "this"
            generator.Emit(OpCodes.Call, proxyType.BaseType.GetConstructor(Type.EmptyTypes)); // Call base class constructor
            generator.Emit(OpCodes.Ldarg_0); // Load "this"
            generator.Emit(OpCodes.Ldarg_1); // Load argument
            generator.Emit(OpCodes.Stfld, hostNameField); // Store in the field
            generator.Emit(OpCodes.Ldarg_0); // Load "this"
            generator.Emit(OpCodes.Ldarg_2); // Load argument
            generator.Emit(OpCodes.Stfld, portField); // Store in the field
            generator.Emit(OpCodes.Ldarg_0); // Load "this"
            generator.Emit(OpCodes.Ldarg_3); // Load argument
            generator.Emit(OpCodes.Stfld, objectNameField); // Store in the field
            generator.Emit(OpCodes.Ret);
        }
    }
}
