using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{

    public delegate T RcSource<T>();

    public delegate void InitiateRc(int interval, int timeout);

    public interface ReactiveComputation
    {
        //int IdNumber { get; }
        void KeepAlive(int interval = 5000, int timeout = 0);
    }

    public class ReactiveComputation<TResult> : ReactiveComputation, IRcCacheObserver
    {
        //public int IdNumber { get; private set; }

        //InvokeMethodOptions InvokeOptions;
        int KeepAliveInterval;
        int KeepAliveTimeout;


        //Task<TResult> UpdateTask;
        //CancellationTokenSource CancellationTokenSource;

        InitiateRc StartReactiveComputation;
       
        List<RcEnumeratorAsync<TResult>> Observers;


        public ReactiveComputation(InitiateRc initiate)
        {
            //IdNumber = RuntimeClient.Current.RcManager.NewId();
            StartReactiveComputation = initiate;
            Observers = new List<RcEnumeratorAsync<TResult>>();
            //SetUpdateTask();
        }

        public RcEnumeratorAsync<TResult> GetAsyncEnumerator()
        {
            var Enumerator = new RcEnumeratorAsync<TResult>();
            Observers.Add(Enumerator);
            return Enumerator;
        }

        public Task OnNext(object result)
        {
            return Task.WhenAll(Observers.Select(o => o.OnNext(result)));
        }

       
        //private void SetUpdateTask()
        //{
        //    CancellationTokenSource = new CancellationTokenSource();
        //    UpdateTask = new Task<TResult>(() => {
        //        return Result;
        //    }, CancellationTokenSource.Token);
        //}

        //public string GetKey()
        //{
        //    return IdNumber.ToString();
        //}

        #region user interface

        public void KeepAlive(int interval = 5000, int timeout = 0)
        {
            KeepAliveInterval = interval;
            KeepAliveTimeout = (timeout == 0 ) ? interval * 2 : timeout;
            StartReactiveComputation(KeepAliveInterval, KeepAliveTimeout);
            //InitiateQuery(KeepAliveInterval, KeepAliveTimeout, this);
        }
        //public new void Cancel()
        //{
        //    CancellationTokenSource.Cancel();
        //    SetUpdateTask();
        //}

        #endregion

    }
}


