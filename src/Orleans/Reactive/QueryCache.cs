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
    interface QueryCache
    {
        Task TriggerUpdate(object result);
        IEnumerable<Message> GetPushMessages();

    }
    class QueryCache<TResult> : QueryCache
    {
        private InvokeMethodRequest Request;
        private InvokeMethodOptions Options;
        private IAddressable Target;
        private bool IsRoot = false;

        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstReceived;
        public TResult Result { get; private set; }

        private ConcurrentDictionary<string, IQueryCacheObserver> Observers = new ConcurrentDictionary<string, IQueryCacheObserver>();

        // TODO
        private List<PullDependency> PullsFrom = new List<PullDependency>();

        public QueryCache(InvokeMethodRequest request, IAddressable target, bool isRoot)
        {
            IsRoot = isRoot;
            Request = request;
            Target = target;
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstReceived = Tcs.Task;
        }


        public void SetResult(TResult result)
        {
            Result = result;
        }

        // TODO: better solution for this.
        // We need it because it's possible that the
        // same query is executed multiple time within the same parent query or within or parent queries
        public void TriggerInitialResult(TResult result)
        {
            try
            {
                SetResult(result);
                Tcs.TrySetResult(result);
                //TriggerUpdate(result);
            } catch (Exception e)
            {
                throw e;
            }
        }

        public Task TriggerUpdate(object result)
        {
            SetResult((TResult)result);

            // update all queries and queryinternals
            var UpdateTasks = Observers.Values.Select(o => o.OnNext(result));
            return Task.WhenAll(UpdateTasks);
        }

        public IEnumerable<Message> GetPushMessages()
        {
            return Observers.Values.SelectMany(o =>
            {
                var QueryInternal = o as QueryInternal;
                if (QueryInternal != null)
                {
                    return QueryInternal.GetPushMessages();
                }
                return Enumerable.Empty<Message>();
            });
        }

        public bool TrySubscribe(IQueryCacheObserver observer)
        {
            return Observers.TryAdd(observer.GetKey(), observer);
        }


        public bool HasObserver(IQueryCacheObserver observer)
        {
            return Observers.ContainsKey(observer.GetKey());
        }

        //public void AddParentQuery(QueryInternal Query)
        //{
        //    ParentQueries.Add(Query.GetFullKey(), Query);
        //    // TODO: Notify QueryInternals you push to about this parent!!
        //    // (PullsFrom)
        //}
    }
}
