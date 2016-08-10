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
        //Task TriggerUpdate(object result);
        //IEnumerable<Message> GetPushMessages(GrainId grainId);
        // void RecalculateDependants(GrainId grainId);
        //RcEnumeratorAsync GetEnumerator(GrainId grainId);
        void OnNext(GrainId grainId, object result, Exception exception = null);

        void OnNext(byte[] result, Exception exception = null);
    }

        class RcCache<TResult>: RcCache
    {

        public TResult Result { get; set; }
        public Exception ExceptionResult { get; private set; }

        private ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>> Enumerators;

        public RcCache()
        {
            Enumerators = new ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>>();
        }

        public void OnNext(GrainId grainId, object result, Exception exception) {
            Result = (TResult)result;
            ExceptionResult = exception;
            Dictionary<string, RcEnumeratorAsync<TResult>> EnumeratorsForGrain;
            Enumerators.TryGetValue(grainId, out EnumeratorsForGrain);
            if (EnumeratorsForGrain != null)
            {
                foreach (var Enumerator in EnumeratorsForGrain.Values)
                {
                    Enumerator.OnNext(result, exception);
                }
            }
        }

        public void OnNext(byte[] result, Exception exception = null)
        {
            Result = Serialization.SerializationManager.Deserialize<TResult>(new BinaryTokenStreamReader(result));

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
            if (Result != null || ExceptionResult != null)
            {
                Enumerator.OnNext(Result, ExceptionResult);
            }
            return Enumerator;
        }
    }
}
