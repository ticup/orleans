using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    interface RcCache
    {
        void RemoveDependencyFor(RcSummaryBase dependingSummary);
        void OnNext(byte[] result, Exception exception = null);
    }

    enum RcCacheStatus
    {
        NotYetReceived,
        HasResult,
        Exception
    }

        class RcCache<TResult>: RcCache, IDisposable
    {

        private string Key;
        private RcCacheStatus State;
        public TResult Result { get; set; }
        public Exception ExceptionResult { get; private set; }

        // Keeps track of the RcSummaries that depend on this cache
        // Maps the FullKey of an RcSummary to a single RcEnumeratorAsync for that Summary.
        private ConcurrentDictionary<string, RcEnumeratorAsync<TResult>> Enumerators;

        private RcManager RcManager;

        private InvokeMethodOptions Options;
        private InvokeMethodRequest Request;
        private GrainReference Grain;
        private TimeSpan Refresh;

        private IDisposable Timer;
        private System.Timers.Timer STimer;


        public RcCache(RcManager rcManager, string cacheKey, GrainReference grain, InvokeMethodRequest request, InvokeMethodOptions options, TimeSpan refresh)
        {
            Enumerators = new ConcurrentDictionary<string, RcEnumeratorAsync<TResult>>();
            State = RcCacheStatus.NotYetReceived;
            RcManager = rcManager;
            Key = cacheKey;
            Request = request;
            Options = options;
            Grain = grain;
            Refresh = refresh;
        }

        public void Dispose()
        {
            RcManager.Logger.Verbose("Disposing Cache ", this.Key);
            if (Timer != null)
            {
                Timer.Dispose();
            }
            if (STimer != null)
            {
                STimer.Stop();
                STimer.Dispose();
            }
        }

        internal void StartTimer()
        {
            if (Timer != null)
            {
                throw new OrleansException("Can only create one timer per cache");
            }
            if (RuntimeClient.Current.CurrentActivationData != null)
            {
                RcManager.Logger.Verbose("Cache {0} : Setting up timer that will keep cache alive every {1} ms", this.Key, Refresh.TotalMilliseconds);
                Timer = RuntimeClient.Current.CurrentActivationData.RegisterTimer(_ =>
                {
                    Grain.RefreshSubscription(Request, Options);
                    return TaskDone.Done;
                }, null, Refresh, Refresh);

            }
            else
            {
                RcManager.Logger.Verbose("Cache {0} : Setting up timer that will keep cache alive every {1} ms", this.Key, Refresh.TotalMilliseconds);
                STimer = new System.Timers.Timer();
                STimer.Interval = Refresh.TotalMilliseconds;
                STimer.Elapsed += new System.Timers.ElapsedEventHandler((o, e) =>
                {
                    Grain.RefreshSubscription(Request, Options);
                });
                STimer.Start();
            }
        }

        public bool HasValue()
        {
            return this.State != RcCacheStatus.NotYetReceived;
        }

        public void OnNext(byte[] result, Exception exception = null)
        {
            // TODO: Do another comparison, so that the server can do redundant pushes.
            if (exception != null)
            {
                State = RcCacheStatus.Exception;
                Result = default(TResult);
                ExceptionResult = exception;

            } else
            {
                State = RcCacheStatus.HasResult;
                Result = Serialization.SerializationManager.Deserialize<TResult>(new BinaryTokenStreamReader(result));
                ExceptionResult = null;
                if (exception is ComputationStopped)
                {
                    Grain.InitiateQuery(Request, Options);
                }
            }

            foreach (var kvp in Enumerators)
                kvp.Value.OnNext(Result, exception);
        }


        /// <summary>
        /// Gets an <see cref="RcEnumeratorAsync"/> that produces values for this cache.
        /// This enumerator is dedicated to the given Summary and only 1 per summary will be created.
        /// </summary>
        /// <param name="dependingSummary">The <see cref="RcSummary"/> for which the enumerator must be created</param>
        /// <returns>True if this cache was not concurrently removed from the <see cref="InsideRcManager.CacheMap"/> while retrieving the enumerator</returns>
        public bool GetEnumeratorAsync(RcSummaryBase dependingSummary, out RcEnumeratorAsync<TResult> enumerator)
        {
            var DependingKey = dependingSummary.GetFullKey();
            var Enumerator1 = new RcEnumeratorAsync<TResult>();
            lock (Enumerators)
            {
                var cache = RcManager.GetCache(Key);
                if (cache != this)
                {
                    enumerator = null;
                    return false;
                }
                enumerator = Enumerators.GetOrAdd(DependingKey, Enumerator1);
            }
           
            var existed = enumerator != Enumerator1;

            if (!existed && HasValue())
            {
                enumerator.OnNext(Result, ExceptionResult);
            }
            return true;
        }

        public void RemoveDependencyFor(RcSummaryBase dependingSummary)
        {
            RcEnumeratorAsync<TResult> RcEnumeratorAsync;
            RcManager.Logger.Verbose("RcCache {0} : Removing Summary {1} as a dependency ({2} dependencies)", this.Key, dependingSummary, Enumerators.Count);
            lock (Enumerators)
            {
                Enumerators.TryRemove(dependingSummary.GetFullKey(), out RcEnumeratorAsync);
                if (Enumerators.Count == 0)
                {
                    RcManager.RemoveCache(Key);
                    RcManager.Logger.Verbose("Removed cache {0}", Key);
                    this.Dispose();
                }
            }
            RcEnumeratorAsync.OnNext(null, new ComputationStopped());
        }

        public override string ToString()
        {
            return "Cache " + Key;
        }

    }
}
