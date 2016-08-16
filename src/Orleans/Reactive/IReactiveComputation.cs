using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Reactive
{


    /// <summary>
    /// A reactive computation that automatically refreshes its result.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    public interface IReactiveComputation<TResult>
    {

        /// <summary>
        /// Returns an enumerator for receiving successive versions of results of the computation.
        /// </summary>
        /// <returns>An enumerator object</returns>
        IResultEnumerator<TResult> GetResultEnumerator();

    }



}
