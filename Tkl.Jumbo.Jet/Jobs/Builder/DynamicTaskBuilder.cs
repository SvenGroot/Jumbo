// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection.Emit;
using System.Reflection;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Tkl.Jumbo.Jet.Tasks;

namespace Tkl.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Allows you to create task classes from static methods.
    /// </summary>
    public sealed class DynamicTaskBuilder
    {
        private AssemblyBuilder _assembly;
        private ModuleBuilder _module;
        private string _dynamicAssemblyDirectory;

        /// <summary>
        /// Creates a dynamically generated task class by overriding the specified method.
        /// </summary>
        /// <param name="methodToOverride">The method to override.</param>
        /// <param name="taskMethodDelegate">Delegate for the method that the implementation of <paramref name="methodToOverride"/> will call.</param>
        /// <param name="skipParameters">The number of parameters of <paramref name="methodToOverride"/> to skip before passing parameters on to the delegate method.</param>
        /// <param name="recordReuseMode">The record reuse mode.</param>
        /// <returns>The <see cref="Type"/> instance for the dynamically generated type.</returns>
        /// <remarks>
        /// <para>
        ///   The <see cref="MemberInfo.DeclaringType"/> of <paramref name="methodToOverride"/> will become the base type of the dynamic task type. If the <see cref="MemberInfo.DeclaringType"/> is
        ///   an interface, the base type will be <see cref="Configurable"/> and the type will implement the specified interface. The interface or base type
        ///   may not have any other methods that need to be overridden.
        /// </para>
        /// <para>
        ///   The target method for <paramref name="taskMethodDelegate"/> must match the signature of the <paramref name="methodToOverride"/>, minus
        ///   <paramref name="skipParameters"/> parameters at the start. It may optionally take an additional parameter of type <see cref="TaskContext"/>.
        /// </para>
        /// <para>
        ///   If the target method for <paramref name="taskMethodDelegate"/> is not public, you must add the delegate to the setting's for the
        ///   stage in which this task is used by using the <see cref="SerializeDelegate"/> method.
        /// </para>
        /// </remarks>
        public Type CreateDynamicTask(MethodInfo methodToOverride, Delegate taskMethodDelegate, int skipParameters, RecordReuseMode recordReuseMode)
        {
            if( methodToOverride == null )
                throw new ArgumentNullException("methodToOverride");
            if( methodToOverride.DeclaringType.FindGenericInterfaceType(typeof(ITask<,>), false) == null )
                throw new ArgumentException("The method that declares the method to override is not a task.", "methodToOverride");
            if( taskMethodDelegate == null )
                throw new ArgumentNullException("taskMethodDelegate");
            if( !taskMethodDelegate.Method.IsStatic )
                throw new ArgumentException("The delegate method must be static.", "taskMethodDelegate");
            ParameterInfo[] parameters = methodToOverride.GetParameters();
            ParameterInfo[] delegateParameters = taskMethodDelegate.Method.GetParameters();
            ValidateParameters(skipParameters, parameters, delegateParameters);

            FieldBuilder delegateField;
            TypeBuilder taskType = CreateTaskType(taskMethodDelegate, recordReuseMode, methodToOverride.DeclaringType, out delegateField);
            MethodBuilder overriddenMethod = OverrideMethod(taskType, methodToOverride);

            ILGenerator generator = overriddenMethod.GetILGenerator();
            if( !taskMethodDelegate.Method.IsPublic )
            {
                generator.Emit(OpCodes.Ldarg_0); // Put "this" on the stack
                generator.Emit(OpCodes.Ldfld, delegateField); // Put the delegate on the stack.
            }

            for( int x = skipParameters; x < parameters.Length; ++x )
                generator.Emit(OpCodes.Ldarg, x + 1); // Zero is "this", hence +1
            if( delegateParameters.Length > parameters.Length - skipParameters )
            {
                // Put the TaskContext on the stack.
                generator.Emit(OpCodes.Ldarg_0);
                generator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod());
            }
            if( taskMethodDelegate.Method.IsPublic )
                generator.Emit(OpCodes.Call, taskMethodDelegate.Method);
            else
                generator.Emit(OpCodes.Callvirt, taskMethodDelegate.GetType().GetMethod("Invoke"));
            generator.Emit(OpCodes.Ret);

            return taskType.CreateType();
        }

        /// <summary>
        /// Serializes a delegate to the specified <see cref="SettingsDictionary"/>.
        /// </summary>
        /// <param name="settings">The settings.</param>
        /// <param name="taskDelegate">The task delegate.</param>
        /// <remarks>
        /// <para>
        ///   If you've used the <see cref="CreateDynamicTask"/> method and your delegate's target method is not public, use
        ///   this method to serialize the delegate to use to call the method and store it in the job settings.
        /// </para>
        /// </remarks>
        public static void SerializeDelegate(SettingsDictionary settings, Delegate taskDelegate)
        {
            if( settings == null )
                throw new ArgumentNullException("settings");
            if( taskDelegate == null )
                throw new ArgumentNullException("taskDelegate");
            BinaryFormatter formatter = new BinaryFormatter();
            using( MemoryStream stream = new MemoryStream() )
            {
                formatter.Serialize(stream, taskDelegate);
                settings.Add(TaskConstants.JobBuilderDelegateSettingKey, Convert.ToBase64String(stream.ToArray()));
            }
        }
        
        /// <summary>
        /// Deserializes a delegate. This method is for internal Jumbo use only.
        /// </summary>
        /// <param name="context">The task context.</param>
        /// <returns></returns>
        public static object DeserializeDelegate(TaskContext context)
        {
            if( context != null )
            {
                string base64Delegate = context.StageConfiguration.GetSetting(TaskConstants.JobBuilderDelegateSettingKey, null);
                if( base64Delegate != null )
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    byte[] serializedDelegate = Convert.FromBase64String(base64Delegate);
                    using( MemoryStream stream = new MemoryStream(serializedDelegate) )
                    {
                        return formatter.Deserialize(stream);
                    }
                }
            }

            return null;
        }

        internal bool IsDynamicAssembly(Assembly assembly)
        {
            return object.Equals(assembly, _assembly);
        }

        private void CreateDynamicAssembly()
        {
            if( _assembly == null )
            {
                // Use a Guid to ensure a unique name.
                AssemblyName name = new AssemblyName("Tkl.Jumbo.Jet.Generated." + Guid.NewGuid().ToString("N"));
                _dynamicAssemblyDirectory = Path.GetTempPath();
                _assembly = AppDomain.CurrentDomain.DefineDynamicAssembly(name, AssemblyBuilderAccess.RunAndSave, _dynamicAssemblyDirectory);
                _module = _assembly.DefineDynamicModule(name.Name, name.Name + ".dll");
            }
        }

        private static void ValidateParameters(int skipParameters, ParameterInfo[] parameters, ParameterInfo[] delegateParameters)
        {
            if( skipParameters < 0 || skipParameters > parameters.Length )
                throw new ArgumentOutOfRangeException("skipParameters");
            if( delegateParameters.Length < parameters.Length - skipParameters || delegateParameters.Length > parameters.Length - skipParameters + 1 )
                throw new ArgumentException("The delegate method doesn't have the correct number of parameters.");
            for( int x = 0; x < delegateParameters.Length; ++x )
            {
                Type requiredType = (x + skipParameters == parameters.Length) ? typeof(TaskContext) : parameters[x + skipParameters].ParameterType;
                if( delegateParameters[x].ParameterType != requiredType )
                    throw new ArgumentException("The delegate method doesn't have the correct method signature.");
            }
        }

        private static void SetTaskAttributes(MethodInfo taskMethod, RecordReuseMode mode, TypeBuilder taskTypeBuilder)
        {
            if( mode != RecordReuseMode.DontAllow )
            {
                Type allowRecordReuseAttributeType = typeof(AllowRecordReuseAttribute);
                AllowRecordReuseAttribute allowRecordReuse = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(taskMethod, allowRecordReuseAttributeType);
                if( mode == RecordReuseMode.Allow || mode == RecordReuseMode.PassThrough || allowRecordReuse != null )
                {
                    ConstructorInfo ctor = allowRecordReuseAttributeType.GetConstructor(Type.EmptyTypes);
                    PropertyInfo passThrough = allowRecordReuseAttributeType.GetProperty("PassThrough");

                    CustomAttributeBuilder allowRecordReuseBuilder = new CustomAttributeBuilder(ctor, new object[] { }, new[] { passThrough }, new object[] { mode == RecordReuseMode.PassThrough || (allowRecordReuse != null && allowRecordReuse.PassThrough) });
                    taskTypeBuilder.SetCustomAttribute(allowRecordReuseBuilder);
                }
            }

            if( Attribute.IsDefined(taskMethod, typeof(ProcessAllInputPartitionsAttribute)) )
            {
                ConstructorInfo ctor = typeof(ProcessAllInputPartitionsAttribute).GetConstructor(Type.EmptyTypes);
                CustomAttributeBuilder partitionAttribute = new CustomAttributeBuilder(ctor, new object[0]);

                taskTypeBuilder.SetCustomAttribute(partitionAttribute);
            }
        }

        private TypeBuilder CreateTaskType(Delegate taskDelegate, RecordReuseMode recordReuseMode, Type baseOrInterfaceType, out FieldBuilder delegateField)
        {
            CreateDynamicAssembly();

            Type[] interfaces = null;
            if( baseOrInterfaceType.IsInterface )
            {
                interfaces = new[] { baseOrInterfaceType };
                baseOrInterfaceType = typeof(Configurable);
            }

            TypeBuilder taskTypeBuilder = _module.DefineType(_assembly.GetName().Name + "." + taskDelegate.Method.Name, TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed, baseOrInterfaceType, interfaces);

            SetTaskAttributes(taskDelegate.Method, recordReuseMode, taskTypeBuilder);

            if( !taskDelegate.Method.IsPublic )
                delegateField = CreateDelegateField(taskDelegate, taskTypeBuilder);
            else
                delegateField = null;

            return taskTypeBuilder;
        }

        private static FieldBuilder CreateDelegateField(Delegate taskDelegate, TypeBuilder taskTypeBuilder)
        {
            FieldBuilder delegateField;
            delegateField = taskTypeBuilder.DefineField("_taskFunction", taskDelegate.GetType(), FieldAttributes.Private);
            MethodBuilder configMethod = taskTypeBuilder.DefineMethod("NotifyConfigurationChanged", MethodAttributes.Public | MethodAttributes.Virtual);
            ILGenerator configGenerator = configMethod.GetILGenerator();
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for stfld)
            configGenerator.Emit(OpCodes.Call, taskTypeBuilder.BaseType.GetMethod("NotifyConfigurationChanged"));
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for stfld)
            configGenerator.Emit(OpCodes.Ldarg_0); // Put this on stack (for call)
            configGenerator.Emit(OpCodes.Call, typeof(Configurable).GetProperty("TaskContext").GetGetMethod()); // Put task context on stack
            configGenerator.Emit(OpCodes.Call, typeof(DynamicTaskBuilder).GetMethod("DeserializeDelegate")); // Call deserialize method
            configGenerator.Emit(OpCodes.Castclass, taskDelegate.GetType());
            configGenerator.Emit(OpCodes.Stfld, delegateField);
            configGenerator.Emit(OpCodes.Ret);
            return delegateField;
        }

        private MethodBuilder OverrideMethod(TypeBuilder taskTypeBuilder, MethodInfo interfaceMethod)
        {
            ParameterInfo[] parameters = interfaceMethod.GetParameters();
            MethodBuilder method = taskTypeBuilder.DefineMethod(interfaceMethod.Name, MethodAttributes.Public | MethodAttributes.Virtual, interfaceMethod.ReturnType, parameters.Select(p => p.ParameterType).ToArray());
            foreach( ParameterInfo parameter in parameters )
            {
                method.DefineParameter(parameter.Position, parameter.Attributes, parameter.Name);
            }

            return method;
        }
    }
}
