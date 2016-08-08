using Orleans.CodeGeneration;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    interface RcCache
    {
        //Task TriggerUpdate(object result);
        //IEnumerable<Message> GetPushMessages(GrainId grainId);
        // void RecalculateDependants(GrainId grainId);
        //RcEnumeratorAsync GetEnumerator(GrainId grainId);
        void OnNext(GrainId grainId, object result);
    }

        class RcCache<TResult>: RcCache
    {

        public TResult Result { get; set; }
        private ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>> Enumerators;

        public RcCache()
        {
            Enumerators = new ConcurrentDictionary<GrainId, Dictionary<string, RcEnumeratorAsync<TResult>>>();
        }


        //public void SetResult(TResult result)
        //{
        //    Result = result;
        //}

        // TODO: better solution for this?
        // We need it because it's possible that the
        // same computation is executed multiple time within the same or another parent computation
        //public void TriggerInitialResult(TResult result)
        //{
        //    SetResult(result);
        //    //TriggerUpdate(result);
        //}

        //public Task TriggerUpdate(object result)
        //{
        //    SetResult((TResult)result);
        //    // TODO: better solution? This only has to happen first time.
        //    // Could use a boolean, but is same overhead.
        //    //Tcs.TrySetResult((TResult)result); 
        //    var UpdateTasks = Observers.Values.Select(o => o.OnNext(result));
        //    return Task.WhenAll(UpdateTasks);
        //}

        //public IEnumerable<Message> GetPushMessages(GrainId grainId)
        //{
        //    Dictionary<string, RcSummary> DependentsForGrain;
        //    Dependents.TryGetValue(grainId, out DependentsForGrain);
        //    if (DependentsForGrain == null)
        //    {
        //        return Enumerable.Empty<Message>();
        //    }
        //    return DependentsForGrain.Values.SelectMany(Summary =>
        //    {
        //        return Summary.GetPushMessages();
        //    });
        //}

        //public void RecalculateDependants(GrainId grainId)
        //{
        //    Dictionary<string, RcSummary> DependentsForGrain;
        //    Dependents.TryGetValue(grainId, out DependentsForGrain);
        //    if (DependentsForGrain != null)
        //    {
        //        foreach (var Summary in DependentsForGrain.Values)
        //        {
        //            Summary.Calculate();
        //        }
        //    }
        //}

        public void OnNext(GrainId grainId, object result) {
            Result = (TResult)result;
            Dictionary<string, RcEnumeratorAsync<TResult>> EnumeratorsForGrain;
            Enumerators.TryGetValue(grainId, out EnumeratorsForGrain);
            if (EnumeratorsForGrain != null)
            {
                foreach (var Enumerator in EnumeratorsForGrain.Values)
                {
                    Enumerator.OnNext(result);
                }
            }
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
            var Enumerator = new RcEnumeratorAsync<TResult>();
            var ObserversForGrain = Enumerators.GetOrAdd(grainId, _ => new Dictionary<string, RcEnumeratorAsync<TResult>>());
            ObserversForGrain.Add(dependingSummary.GetLocalKey(), Enumerator);
            if (Result != null)
            {
                Enumerator.OnNext(Result);
            }
            return Enumerator;
        }
    }
}
