﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Reactive;

namespace Orleans.Runtime.Reactive
{
    internal interface RcEnumeratorAsync
    {
        void OnNext(object result, Exception exception = null);
    }

    internal class RcEnumeratorAsync<TResult> : RcEnumeratorAsync, IResultEnumerator<TResult>, IDisposable
    {
        private TResult Result;
        private Exception ExceptionResult;

        private enum ConsumptionStates
        {
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
        public RcEnumeratorAsync(TResult initialresult, Exception exceptionresult)
        {
            Result = initialresult;
            ConsumptionState = ConsumptionStates.Behind;
        }
        public bool NextResultIsReady
        {
            get
            {
                return ConsumptionState == ConsumptionStates.Behind;
            }
        }

        public void OnNext(object result, Exception exception = null)
        {
            TaskCompletionSource<TResult> promise_to_signal = null;
            lock (this)
            {
                Result = result == null ? default(TResult) : (TResult)result;
                ExceptionResult = exception;

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
                if (exception == null)
                {
                    promise_to_signal.SetResult((TResult)result);
                }
                else
                {
                    promise_to_signal.SetException(exception);
                }
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
                            if (ExceptionResult == null)
                            {
                                return Task.FromResult(Result);
                            }
                            else
                            {
                                var completion = new TaskCompletionSource<TResult>();
                                completion.SetException(ExceptionResult);
                                return completion.Task;
                            }
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

        public void Dispose()
        {

        }
    }
}