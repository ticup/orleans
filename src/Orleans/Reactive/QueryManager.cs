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

        // Double Map of "interfaceId.grainId" -> "methodId(args)" -> InternalQuery
        // TODO: This should be pushed down into the GrainActivation
        Dictionary<string, Dictionary<string, QueryInternal>> InternalQueryMap;

        // Keeps track of cached summaries across and entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        ConcurrentDictionary<string, QueryCache> QueryCacheMap;

        public QueryInternal CurrentQuery { get; set; }

        public QueryManager()
        {
            InternalQueryMap = new Dictionary<string, Dictionary<string, QueryInternal>>();
            QueryCacheMap    = new ConcurrentDictionary<string, QueryCache>();

            CurrentQuery = null;
        }

        public static string GetInternalQueryKey(GrainId grainId, int interfaceId)
        {
            return interfaceId + "#" + grainId;
        }
        public static string GetFullMethodKey(GrainId gid, int InterfaceId, int MethodId, object[] Arguments)
        {
            return gid + "#" + InterfaceId + "." + MethodId + "(" + Utils.EnumerableToString(Arguments) + ")";
        }

        public static string GetFullMethodKey(GrainId gId, InvokeMethodRequest request)
        {
            return GetFullMethodKey(gId, request.InterfaceId, request.MethodId, request.Arguments);
        }

        public static string GetMethodAndArgsKey(InvokeMethodRequest request)
        {
            return GetMethodAndArgsKey(request.MethodId, request.Arguments);
        }

        public static string GetMethodAndArgsKey(int methodId, object[] args)
        {
            return methodId +"(" + Utils.EnumerableToString(args) + ")";
        }

        public bool IsQuerying() {
            return CurrentQuery != null;
        }

        public QueryCache<T> GetCache<T>(GrainId gId, InvokeMethodRequest request)
        {
            return (QueryCache<T>)GetCache(gId, request);
        }


        #region Thread-safe access for caches
        public QueryCache GetCache(GrainId gId, InvokeMethodRequest request)
        {
            QueryCache Cache;
            var Key = GetFullMethodKey(gId, request);
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

        public bool TryAddCache(GrainId gId, InvokeMethodRequest request, QueryCache cache)
        {
            try
            {
                var Key = GetFullMethodKey(gId, request);
                return QueryCacheMap.TryAdd(Key, cache);
                // DEBUG purposes
            }
            catch (Exception e)
            {
                throw e;
            }
        }
        #endregion



        public QueryInternal<T> GetOrAddPushingQuery<T>(GrainId grainId, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout,  Message message, bool isRoot)
        {
            try
            {
                QueryInternal<T> InternalQuery = Get<T>(grainId, request);
                bool ExistingQuery = InternalQuery != null;
                if (!ExistingQuery)
                {
                    InternalQuery = new QueryInternal<T>(message.TargetGrain, request, target, invoker, message.SendingSilo, message.SendingGrain, message.SendingActivation, timeout, isRoot);
                    Add(InternalQuery);
                }
                else
                {
                    InternalQuery.GetOrAddPushDependency(message.SendingSilo, message.SendingGrain, message.SendingActivation, timeout);
                }
                return InternalQuery;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public void Add(QueryInternal Query)
        {
            try
            {
                var key = GetInternalQueryKey(Query.GetGrainId(), Query.GetInterfaceId());
                var InterfaceId = Query.GetInterfaceId();
                Dictionary<string, QueryInternal> GrainMap;
                InternalQueryMap.TryGetValue(key, out GrainMap);
                if (GrainMap == null)
                {
                    GrainMap = new Dictionary<string, QueryInternal>();
                    InternalQueryMap.Add(key, GrainMap);
                }
                GrainMap.Add(Query.GetMethodAndArgsKey(), Query);
            } catch (Exception e)
            {
                throw e;
            }
        }

        public QueryInternal<T> Get<T>(GrainId grainId, InvokeMethodRequest request)
        {
            return (QueryInternal<T>)Get(grainId, request);
        }

        public QueryInternal Get(GrainId grainId, InvokeMethodRequest request)
        {
            Dictionary<string, QueryInternal> GrainMap;
            var key = GetInternalQueryKey(grainId, request.InterfaceId);

            InternalQueryMap.TryGetValue(key, out GrainMap);
            if (GrainMap != null)
            {
                QueryInternal Query;
                GrainMap.TryGetValue(GetMethodAndArgsKey(request), out Query);
                return Query;
            }
            return null;
        }

        //public void Add(Query Query)
        //{
        //    QueryMap.Add(Query.GetKey(), Query);
        //}

        //public IEnumerable<Message> Update<T>(InvokeMethodRequest request, T result)
        //{
        //    InternalQuery<T> Query = (InternalQuery<T>) Get(request);
        //    return Query.TriggerUpdate(result);
        //}


        // Updates the cache of a query, identified by GrainId and the InvokeMethodRequest, with given result.
        // Dependencies are notified and updated because they observe this cache.
        // Dependency updates happen before this Task returns.
        public Task Update(GrainId gId, InvokeMethodRequest request, object result)
        {
            QueryCache Cache = GetCache(gId, request);
            return Cache.TriggerUpdate(result);
            //Type arg_type = result.GetType();
            //Type class_type = typeof(QueryCache<>);
            //Type class_type2 = class_type.MakeGenericType(new Type[] { arg_type });
            //MethodInfo mi = class_type2.GetMethod("TriggerUpdate");
            //return (Task)mi.Invoke(Cache, new object[] { result });
        }

        public IEnumerable<Message> GetPushMessages(GrainId gId, InvokeMethodRequest request)
        {
            QueryCache Cache = GetCache(gId, request);
            return Cache.GetPushMessages();
        }

        public async Task<IEnumerable<IEnumerable<Message>>> RecalculateQueries(GrainId grainId, int interfaceId)
        {
            Dictionary<string, QueryInternal> GrainMap;
            var key = GetInternalQueryKey(grainId, interfaceId);
            InternalQueryMap.TryGetValue(key, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Recalculate());
                await Task.WhenAll(Tasks);

                return GrainMap.Values.Select(q => q.GetPushMessages());
            }
            return Enumerable.Empty<IEnumerable<Message>>();
        }

        public int NewId()
        {
            return ++IdSequence;
        }

        // When new update comes in:
        // resultTask = OrleansTaskExtentions.ConvertTaskViaTcs(resultTask);
        //    return resultTask.Unbox<T>();
    }
}