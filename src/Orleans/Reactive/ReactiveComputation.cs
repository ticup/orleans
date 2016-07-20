﻿using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{

    public delegate T RcSource<T>();

    //public delegate void InitiateRc(int interval, int timeout);

    public interface ReactiveComputation
    {
        //int IdNumber { get; }
        //void KeepAlive(int interval = 5000, int timeout = 0);
    }

    public class ReactiveComputation<TResult> : ReactiveComputation, IRcCacheObserver
    {
        TResult Result;
       
        List<RcEnumeratorAsync<TResult>> Observers;


        public ReactiveComputation(TResult result)
        {
            Observers = new List<RcEnumeratorAsync<TResult>>();
            Result = result;
        }

        public RcEnumeratorAsync<TResult> GetAsyncEnumerator()
        {
            var Enumerator = new RcEnumeratorAsync<TResult>(Result);
            Observers.Add(Enumerator);
            return Enumerator;
        }

        public Task OnNext(object result)
        {
            Result = (TResult)result;
            return Task.WhenAll(Observers.Select(o => o.OnNext(result)));
        }

    }
}


