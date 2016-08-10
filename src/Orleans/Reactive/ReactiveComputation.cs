﻿using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{



    public interface ReactiveComputation
    {
        //int IdNumber { get; }
        //void KeepAlive(int interval = 5000, int timeout = 0);
    }


    public class ReactiveComputation<TResult> : ReactiveComputation
    {
        TResult Result;
        Exception ExceptionResult;     
        bool HasInitialResult;
        List<RcEnumeratorAsync<TResult>> Observers;

        internal ReactiveComputation()
        {
            Observers = new List<RcEnumeratorAsync<TResult>>();
        }

        internal ReactiveComputation(TResult initialresult) : base()
        {
            Result = initialresult;
            HasInitialResult = true;
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


    }
}


