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
        Task TriggerUpdate(object result);
        IEnumerable<Message> GetPushMessages();

    }
    class RcCache<TResult> : RcCache
    {
        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstReceived;
        public TResult Result { get; private set; }

        private ConcurrentDictionary<string, IRcCacheObserverWithKey> Observers = new ConcurrentDictionary<string, IRcCacheObserverWithKey>();

        public RcCache()
        {
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstReceived = Tcs.Task;
        }


        public void SetResult(TResult result)
        {
            Result = result;
        }

        // TODO: better solution for this?
        // We need it because it's possible that the
        // same computation is executed multiple time within the same or another parent computation
        public void TriggerInitialResult(TResult result)
        {
            SetResult(result);
            Tcs.TrySetResult(result);
            //TriggerUpdate(result);
        }

        public Task TriggerUpdate(object result)
        {
            SetResult((TResult)result);

            var UpdateTasks = Observers.Values.Select(o => o.OnNext(result));
            return Task.WhenAll(UpdateTasks);
        }

        public IEnumerable<Message> GetPushMessages()
        {
            return Observers.Values.SelectMany(o =>
            {
                var RcSummary = o as RcSummary;
                if (RcSummary != null)
                {
                    return RcSummary.GetPushMessages();
                }
                return Enumerable.Empty<Message>();
            });
        }

        public bool TrySubscribe(IRcCacheObserverWithKey observer)
        {
            return Observers.TryAdd(observer.GetKey(), observer);
        }


        public bool HasObserver(IRcCacheObserverWithKey observer)
        {
            return Observers.ContainsKey(observer.GetKey());
        }
    }
}
