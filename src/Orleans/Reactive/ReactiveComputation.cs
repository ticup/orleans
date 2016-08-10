using Orleans.CodeGeneration;
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


    public class ReactiveComputation<TResult> : ReactiveComputation, IRcCacheObserver
    {
        TResult Result;
        Exception ExceptionResult;
        
        List<RcEnumeratorAsync<TResult>> Observers;

        public ReactiveComputation()
        {
            Observers = new List<RcEnumeratorAsync<TResult>>();
        }

        public RcEnumeratorAsync<TResult> GetAsyncEnumerator()
        {
            var Enumerator = new RcEnumeratorAsync<TResult>();
            if (Result != null || ExceptionResult != null)
            {
                Enumerator.OnNext(Result, ExceptionResult);
            }
            Observers.Add(Enumerator);
            return Enumerator;
        }

        public Task OnNext(object result, Exception exception)
        {
            Result = (TResult)result;
            foreach (var e in Observers)
                e.OnNext(result, exception);
            return TaskDone.Done;
        }

    }
}


