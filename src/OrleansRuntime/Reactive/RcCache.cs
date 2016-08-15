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
        void RemoveDependencyFor(RcSummary dependingSummary);

    }

    enum RcCacheStatus
    {
        NotYetReceived,
        HasResult,
        Exception
    }

        class RcCache<TResult>: RcCache
    {

        private string Key;
        private RcCacheStatus State;
        public TResult Result { get; set; }
        public Exception ExceptionResult { get; private set; }

        // Keeps track of the RcSummaries that depend on this cache
        // Maps the FullKey of an RcSummary to a single RcEnumeratorAsync for that Summary.
        private ConcurrentDictionary<string, RcEnumeratorAsync<TResult>> Enumerators;

        private RcManager RcManager;


        public RcCache(RcManager rcManager, string key)
        {
            Enumerators = new ConcurrentDictionary<string, RcEnumeratorAsync<TResult>>();
            State = RcCacheStatus.NotYetReceived;
            RcManager = rcManager;
            Key = key;
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


        /// <summary>
        /// Gets an <see cref="RcEnumeratorAsync"/> that produces values for this cache.
        /// This enumerator is dedicated to the given Summary and only 1 per summary will be created.
        /// </summary>
        /// <param name="dependingSummary">The <see cref="RcSummary"/> for which the enumerator must be created</param>
        /// <returns></returns>
        public RcEnumeratorAsync<TResult> GetEnumeratorAsync(RcSummary dependingSummary)
        {
            var Key = dependingSummary.GetFullKey();
            var Enumerator1 = new RcEnumeratorAsync<TResult>();
            var Enumerator = Enumerators.GetOrAdd(Key, Enumerator1);
            var existed = Enumerator != Enumerator1;

            if (!existed && HasValue())
            {
                Enumerator.OnNext(Result, ExceptionResult);
            }
            return Enumerator;
        }

        public void RemoveDependencyFor(RcSummary dependingSummary)
        {
            RcEnumeratorAsync<TResult> RcEnumeratorAsync;
            Enumerators.TryRemove(dependingSummary.GetFullKey(), out RcEnumeratorAsync);
            lock (Enumerators)
            {
                if (Enumerators.Count == 0)
                {
                    RcManager.RemoveCache(Key);
                }
            }
            
        }
    }
}
