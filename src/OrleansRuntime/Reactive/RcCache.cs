using Orleans.CodeGeneration;
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
        void OnNext(byte[] result, Exception exception = null);
    }

    enum RcCacheState
    {
        NotYetReceived,
        HasResult,
        Exception
    }

        class RcCache<TResult>: RcCache
    {

        private RcCacheState State;
        public TResult Result { get; set; }
        public Exception ExceptionResult { get; private set; }

        private ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>> Enumerators;

        public RcCache()
        {
            Enumerators = new ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>>();
            State = RcCacheState.NotYetReceived;
        }

        public bool HasValue()
        {
            return this.State != RcCacheState.NotYetReceived;
        }

        public void OnNext(byte[] result, Exception exception = null)
        {
            if (exception != null)
            {
                State = RcCacheState.Exception;
                Result = default(TResult);
                ExceptionResult = exception;

            } else
            {
                State = RcCacheState.HasResult;
                Result = Serialization.SerializationManager.Deserialize<TResult>(new BinaryTokenStreamReader(result));
                ExceptionResult = null;
            }

            foreach (var kvp in Enumerators)
                foreach (var e in kvp.Value.Values)
                    e.OnNext(Result, exception);
        }

        public bool HasEnumeratorAsync(GrainId grainId, RcSummary dependingSummary)
        {
            var Enumerator = new RcEnumeratorAsync<TResult>();
            var ObserversForGrain = Enumerators.GetOrAdd(grainId, _ => new Dictionary<string, RcEnumeratorAsync<TResult>>());
            ObserversForGrain.TryGetValue(dependingSummary.GetLocalKey(), out Enumerator);
            return Enumerator != null;
        }

        public RcEnumeratorAsync<TResult> GetEnumeratorAsync(GrainId grainId, RcSummary dependingSummary)
        {
            var ObserversForGrain = Enumerators.GetOrAdd(grainId, _ => new Dictionary<string, RcEnumeratorAsync<TResult>>());
            RcEnumeratorAsync<TResult> Enumerator;
            var key = dependingSummary.GetLocalKey();
            if (! ObserversForGrain.TryGetValue(key, out Enumerator))          
               ObserversForGrain.Add(key, Enumerator = new RcEnumeratorAsync<TResult>());
            if (HasValue())
            {
                Enumerator.OnNext(Result, ExceptionResult);
            }
            return Enumerator;
        }
    }
}
