using Orleans.CodeGeneration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    delegate Task<T> InitiateQueryRequest<T>(Query Query, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);
    class QueryManager
    {
        private int IdSequence = 0;

        // Double Map of "InterfaceId[Activation Key]" -> "methodId(args)" -> Summary
        // TODO: This should be pushed down into the GrainActivation
        ConcurrentDictionary<string, Dictionary<string, QueryInternal>> InternalQueryMap;

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        ConcurrentDictionary<string, QueryCache> QueryCacheMap;

        public QueryInternal CurrentQuery { get; set; }

        public QueryManager()
        {
            InternalQueryMap = new ConcurrentDictionary<string, Dictionary<string, QueryInternal>>();
            QueryCacheMap    = new ConcurrentDictionary<string, QueryCache>();

            CurrentQuery = null;
        }


        public int NewId()
        {
            return ++IdSequence;
        }


        public bool IsQuerying() {
            return CurrentQuery != null;
        }



        #region Thread-safe Cache API
        public IEnumerable<Message> GetPushMessagesForCache(Guid activationKey, InvokeMethodRequest request)
        {
            QueryCache Cache = GetCache(activationKey, request);
            return Cache.GetPushMessages();
        }

        // Updates the cache of a query, identified by GrainId and the InvokeMethodRequest, with given result.
        // Dependencies are notified and updated because they observe this cache.
        // Dependency updates happen before this Task returns.
        public Task UpdateCache(Guid activationKey, InvokeMethodRequest request, object result)
        {
            QueryCache Cache = GetCache(activationKey, request);
            return Cache.TriggerUpdate(result);
        }

        public QueryCache<T> GetCache<T>(Guid activationKey, InvokeMethodRequest request)
        {
            return (QueryCache<T>)GetCache(activationKey, request);
        }

        public QueryCache GetCache(Guid activationKey, InvokeMethodRequest request)
        {
            QueryCache Cache;
            var Key = GetFullMethodKey(activationKey, request);
            QueryCacheMap.TryGetValue(Key, out Cache);
            return Cache;
        }

        //public QueryCache<T> GetOrAddCache<T>(GrainId gId, IAddressable target, InvokeMethodRequest request)
        //{
        //    try { 
        //        var Key = GetFullMethodKey(gId, request);
        //        return (QueryCache<T>)QueryCacheMap.GetOrAdd(Key, (key) => new QueryCache<T>(request, target));
        //    // DEBUG purposes
        //    } catch (Exception e)
        //    {
        //        throw e;
        //    }
        //}

        public bool TryAddCache(Guid activationKey, InvokeMethodRequest request, QueryCache cache)
        {
            try
            {
                var Key = GetFullMethodKey(activationKey, request);
                return QueryCacheMap.TryAdd(Key, cache);
                // DEBUG purposes
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion




        #region Summary API
        public async Task<IEnumerable<IEnumerable<Message>>> RecomputeSummaries(int interfaceId, Guid activationKey)
        {
            Dictionary<string, QueryInternal> GrainMap;
            var Key = GetFullActivationKey(interfaceId, activationKey);
            InternalQueryMap.TryGetValue(Key, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Recalculate());
                await Task.WhenAll(Tasks);

                return GrainMap.Values.Select(q => q.GetPushMessages());
            }
            return Enumerable.Empty<IEnumerable<Message>>();
        }

        public QueryInternal<T> GetSummary<T>(Guid activationKey, InvokeMethodRequest request)
        {
            return (QueryInternal<T>)GetSummary(activationKey, request);
        }

        public QueryInternal GetSummary(Guid activationKey, InvokeMethodRequest request)
        {
            Dictionary<string, QueryInternal> GrainMap;
            var Key = GetFullActivationKey(request.InterfaceId, activationKey);
            InternalQueryMap.TryGetValue(Key, out GrainMap);
            if (GrainMap != null)
            {
                QueryInternal Query;
                GrainMap.TryGetValue(GetMethodAndArgsKey(request), out Query);
                return Query;
            }
            return null;
        }

        public QueryInternal<T> GetOrAddPushingQuery<T>(Guid activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout,  Message message, bool isRoot)
        {
            try
            {
                QueryInternal InternalQuery;
                var Key = GetFullActivationKey(request.InterfaceId, activationKey);
                var ActivationMethodMap = InternalQueryMap.GetOrAdd(Key, k => new Dictionary<string, QueryInternal>());
                var MethodKey = GetMethodAndArgsKey(request);

                ActivationMethodMap.TryGetValue(MethodKey, out InternalQuery);

                if (InternalQuery == null)
                {
                    InternalQuery = new QueryInternal<T>(activationKey, request, target, invoker, message.SendingSilo, message.SendingGrain, message.SendingActivation, message.SendingAddress, timeout, isRoot);
                    ActivationMethodMap.Add(MethodKey, InternalQuery);
                }
                else
                {
                    InternalQuery.GetOrAddPushDependency(message.SendingSilo, message.SendingGrain, message.SendingActivation, message.SendingAddress, timeout);
                }
                    return (QueryInternal<T>)InternalQuery;
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion


        #region Identifier Retrievers

        public static string GetFullActivationKey(int interfaceId, Guid activationKey)
        {
            return interfaceId + "[" + activationKey + "]";
        }

        public static string GetFullMethodKey(Guid activationKey, InvokeMethodRequest request)
        {
            return GetFullActivationKey(request.InterfaceId, activationKey) + "." + GetMethodAndArgsKey(request);
        }


        public static string GetMethodAndArgsKey(InvokeMethodRequest request)
        {
            return request.MethodId + "(" + Utils.EnumerableToString(request.Arguments) + ")";
        }
        
        #endregion

    }
}