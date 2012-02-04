using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Represents information about a type implementing <see cref="ITask{TInput,TOutput}"/>.
    /// </summary>
    public class TaskTypeInfo
    {
        private readonly Type _inputRecordType;
        private readonly Type _outputRecordType;
        private readonly Type _taskType;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskTypeInfo"/> class.
        /// </summary>
        /// <param name="taskType">Type of the task.</param>
        public TaskTypeInfo(Type taskType)
        {
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( taskType.ContainsGenericParameters )
                throw new ArgumentException("The task must be closed constructed generic type.", "taskType");

            _taskType = taskType;
            Type interfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type[] arguments = interfaceType.GetGenericArguments();
            _inputRecordType = arguments[0];
            _outputRecordType = arguments[1];
        }

        /// <summary>
        /// Gets the type of the task.
        /// </summary>
        /// <value>
        /// The type of the task.
        /// </value>
        public Type TaskType
        {
            get { return _taskType; }
        }

        /// <summary>
        /// Gets the type of the input records.
        /// </summary>
        /// <value>
        /// The type of the input records.
        /// </value>
        public Type InputRecordType
        {
            get { return _inputRecordType; }
        }

        /// <summary>
        /// Gets the type of the output records.
        /// </summary>
        /// <value>
        /// The type of the output records.
        /// </value>
        public Type OutputRecordType
        {
            get { return _outputRecordType; }
        }
    }
}
