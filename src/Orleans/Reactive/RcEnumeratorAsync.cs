using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
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



    internal interface RcEnumeratorAsync
    {
        void OnNext(object result);
        //Task<object> OnUpdateAsync();
    }

    // TODO: Disposable: on dispose, notify cache.
    public class RcEnumeratorAsync<TResult> : RcEnumeratorAsync, IResultEnumerator<TResult>
    {
        public TResult Result { get; protected set; }

        private enum ConsumptionStates {
            CaughtUp = 1,
            Behind = 2,
            Ahead = 3
        }

        private ConsumptionStates ConsumptionState;
        TaskCompletionSource<TResult> NextResultPromise; // non-null iff ConsumptionState is Ahead

        public RcEnumeratorAsync()
        {
            ConsumptionState = ConsumptionStates.CaughtUp;
        }
        public RcEnumeratorAsync(TResult initialresult)
        {
            Result = initialresult;
            ConsumptionState = ConsumptionStates.Behind;
        }
        public bool NextResultIsReady {
            get
            {
                return ConsumptionState == ConsumptionStates.Behind;
            }
        }

        public void OnNext(object result)
        {
            TaskCompletionSource<TResult> promise_to_signal = null;
            lock (this)
            {
                Result = (TResult)result;
                switch (ConsumptionState)
                {
                    case ConsumptionStates.Behind:
                        {
                            // remains behind
                            break;
                        }

                    case ConsumptionStates.CaughtUp:
                        {
                            // falls behind
                            ConsumptionState = ConsumptionStates.Behind;
                            break;
                        }

                    case ConsumptionStates.Ahead:
                        {
                            promise_to_signal = NextResultPromise;
                            NextResultPromise = null;
                            ConsumptionState = ConsumptionStates.CaughtUp;
                            break;
                        }
                }
            }

            // we fulfill the promise outside the lock to ensure no continuations execute under the lock
            if (promise_to_signal != null)
            {
                promise_to_signal.SetResult((TResult)result);
            }
        }


        public Task<TResult> NextResultAsync()
        {
            lock (this)
            {
                switch (ConsumptionState)
                {
                    case ConsumptionStates.Behind:
                        {
                            // the current result has not been consumed yet... so return it immediately
                            ConsumptionState = ConsumptionStates.CaughtUp;
                            return Task.FromResult(Result);
                        }

                    case ConsumptionStates.CaughtUp:
                        {
                            // create a promise (to be resolved when we get the next result)
                            NextResultPromise = new TaskCompletionSource<TResult>();
                            ConsumptionState = ConsumptionStates.Ahead;
                            return NextResultPromise.Task;
                        }

                    case ConsumptionStates.Ahead:
                        {
                            // we already have a promise, just return it
                            return NextResultPromise.Task;
                        }

                    default: // should never reach this
                        throw new Runtime.OrleansException("illegal state");
                }
            }
        }

    }
}
