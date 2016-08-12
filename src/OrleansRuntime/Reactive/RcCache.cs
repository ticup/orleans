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

    enum RcCacheStatus
    {
        NotYetReceived,
        HasResult,
        Exception
    }

        class RcCache<TResult>: RcCache
    {

        private RcCacheStatus State;
        public TResult Result { get; set; }
        public Exception ExceptionResult { get; private set; }

        // Keeps track of the RcSummaries that depend on this cache
        // Maps the FullKey of an RcSummary to a single RcEnumeratorAsync for that Summary.
        private ConcurrentDictionary<string, RcEnumeratorAsync<TResult>> Enumerators;

        public RcCache()
        {
            Enumerators = new ConcurrentDictionary<string, RcEnumeratorAsync<TResult>>();
            State = RcCacheStatus.NotYetReceived;
        }

        public bool HasValue()
        {
            return this.State != RcCacheStatus.NotYetReceived;
        }

        public void OnNext(byte[] result, Exception exception = null)
        {
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
            }

            foreach (var kvp in Enumerators)
                kvp.Value.OnNext(Result, exception);
        }

        public RcEnumeratorAsync<TResult> GetEnumeratorAsync(RcSummary dependingSummary)
        {
            var Key = dependingSummary.GetLocalKey();
            var Enumerator = new RcEnumeratorAsync<TResult>();
            var existed = !Enumerators.TryAdd(Key, Enumerator);
            // The enumerator will not be concurrently added/deleted, because
            // 1) only 1 dependingSummary with a given key exists at a time
            // 2) the dependingSummary issues the add/delete and does not run concurently
            if (existed)
            {
                Enumerators.TryGetValue(Key, out Enumerator);
            }
            if (HasValue())
            {
                Enumerator.OnNext(Result, ExceptionResult);
            }
            return Enumerator;
        }
    }
}
