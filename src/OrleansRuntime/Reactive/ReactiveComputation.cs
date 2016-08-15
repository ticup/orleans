﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Reactive;

namespace Orleans.Runtime.Reactive
{
    internal class ReactiveComputation<TResult> : IReactiveComputation<TResult>, IDisposable
    {
        TResult Result;
        Exception ExceptionResult;
        bool HasInitialResult;
        List<RcEnumeratorAsync<TResult>> Observers;
        Action OnDispose;

        internal ReactiveComputation(Action onDispose)
        {
            Observers = new List<RcEnumeratorAsync<TResult>>();
            OnDispose = onDispose;
        }

        public IResultEnumerator<TResult> GetResultEnumerator()
        {
            RcEnumeratorAsync<TResult> enumerator;
            lock (Observers)
            {
                // construct and register enumerator under the lock so we don't miss any results   
                enumerator = HasInitialResult ?
                    new RcEnumeratorAsync<TResult>(Result, ExceptionResult) : new RcEnumeratorAsync<TResult>();
                Observers.Add(enumerator);
            }
            return enumerator;
        }

        public void OnNext(object result, Exception exceptionresult)
        {
            lock (Observers)
            {
                Result = (TResult)result;
                ExceptionResult = exceptionresult;
                HasInitialResult = true;
                foreach (var e in Observers)
                    e.OnNext(result, exceptionresult);
            }
        }

        public TResult LatestOrDefault
        {
            get
            {
                return Result;
            }
        }

        public void Dispose()
        {
            OnDispose();
        }


    }
}