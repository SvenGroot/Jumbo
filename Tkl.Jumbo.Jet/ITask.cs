// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tkl.Jumbo.IO;

namespace Tkl.Jumbo.Jet
{
    /// <summary>
    /// Base interface for task classes. Tasks should implement either <see cref="IPullTask{TInput,TOutput}"/> or <see cref="IPushTask{TInput,TOutput}"/>.
    /// </summary>
    /// <typeparam name="TInput">The input type of the task.</typeparam>
    /// <typeparam name="TOutput">The output type of the task.</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface ITask<TInput, TOutput>
    {
    }
}
