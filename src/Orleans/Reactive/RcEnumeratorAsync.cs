﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{
    public interface RcEnumeratorAsync
    {
        void OnNext(object result, Exception exception = null);
    }

    // TODO: Disposable: on dispose, notify cache.
    public class RcEnumeratorAsync<TResult> : RcEnumeratorAsync
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


        public void OnNext(object result, Exception exception = null)
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
                if (exception == null) {
                    promise_to_signal.SetResult((TResult)result);
                } else
                {
                    promise_to_signal.SetException(exception);
                }
            }
        }


        public Task<TResult> OnUpdateAsync()
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
