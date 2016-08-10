using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Reactive
{

    /// <summary>
    /// An interface for asynchronously enumerating over successive results of a reactive computation.
    /// </summary>
    /// <typeparam name="TResult">The result type</typeparam>
    public interface IResultEnumerator<TResult>
    {
        /// <summary>
        /// Asynchronously waits until a new result is ready, and returns it. 
        /// On the first call, the task completes when the initial result is known. 
        /// On subsequent calls, the task completes only if the result changes.
        /// </summary>
        /// <returns>a task for the latest result</returns>
        Task<TResult> NextResultAsync();

        /// <summary>
        /// Checks if a new result is ready. If true, the next call to <see cref="NextResultAsync"/> 
        /// is guaranteed to execute synchronously.
        /// </summary>
        bool NextResultIsReady { get; }
    }



}
