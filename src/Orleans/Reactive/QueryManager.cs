using Orleans.CodeGeneration;
using System;
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
        // Double Map of interfaceId -> "methodId(args)" -> InternalQuery
        Dictionary<int, Dictionary<string, QueryInternal>> InternalQueryMap;

        Dictionary<string, QueryCache> QueryCacheMap;


        private QueryInternal RunningQuery;

        // TODO: Only for client?
        // Map of query key -> Action that triggers the UpdateTask for a Query
        //Dictionary<string, Query> QueryMap;

        public QueryManager()
        {
            InternalQueryMap = new Dictionary<int, Dictionary<string, QueryInternal>>();
            QueryCacheMap    = new Dictionary<string, QueryCache>();

            RunningQuery = null;
        }

        public static string GetKey(int InterfaceId, int MethodId, object[] Arguments)
        {
            return InterfaceId + "." + MethodId + "(" + Utils.EnumerableToString(Arguments) + ")";
        }

        public static string GetKey(InvokeMethodRequest request)
        {
            return GetKey(request.InterfaceId, request.MethodId, request.Arguments);
        }

        public static string GetMethodAndArgsKey(InvokeMethodRequest request)
        {
            return GetMethodAndArgsKey(request.MethodId, request.Arguments);
        }

        public static string GetMethodAndArgsKey(int methodId, object[] args)
        {
            return methodId +"(" + Utils.EnumerableToString(args) + ")";
        }

        //public InternalQuery<T> GetOrAdd<T>(InvokeMethodRequest request, GrainReference target, bool isRoot)
        //{

        //    InternalQuery<T> Query = Get<T>(request);
        //    if (Query == null)
        //    {
        //        Query = new InternalQuery<T>(request, target, isRoot);
        //    }
        //    return Query;
        //}

        public bool IsQuerying() {
            return RunningQuery != null;
        }

        public void StartQuery(QueryInternal query)
        {
            if (RunningQuery != null)
            {
                throw new Exception("Cannot run a query within a query");
            }
            RunningQuery = query;
        }

        public void StopQuery(QueryInternal query)
        {
            if (RunningQuery != null)
            {
                throw new Exception("No query currently running");
            }
            RunningQuery = null;
        }

        public QueryCache<T> GetQueryCache<T>(InvokeMethodRequest request)
        {
            return (QueryCache<T>)GetQueryCache(request);
        }

        public QueryCache GetQueryCache(InvokeMethodRequest request)
        {
            QueryCache Cache;
            var Key = GetKey(request);
            QueryCacheMap.TryGetValue(Key, out Cache);
            return Cache;
        }

        public void AddQueryCache(InvokeMethodRequest request, QueryCache cache)
        {
            var Key = GetKey(request);
            QueryCacheMap.Add(Key, cache);

        }

        public QueryInternal<T> GetOrAddPushingQuery<T>(IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int dependingId, int timeout,  Message message)
        {
            QueryInternal<T> InternalQuery = Get<T>(request);
            bool ExistingQuery = InternalQuery != null;
            if (!ExistingQuery)
            {
                InternalQuery = new QueryInternal<T>(request, target, invoker, message.SendingSilo, message.SendingGrain, message.SendingActivation, dependingId, timeout, true);
                Add(InternalQuery);
            }
            else
            {
                InternalQuery.AddPushDependency(message.SendingSilo, message.SendingGrain, message.SendingActivation, dependingId, timeout);
            }
            return InternalQuery;
        }

        public void Add(QueryInternal Query)
        {
            var InterfaceId = Query.GetInterfaceId();
            Dictionary<string, QueryInternal> GrainMap;
            InternalQueryMap.TryGetValue(InterfaceId, out GrainMap);
            if (GrainMap == null)
            {
                GrainMap = new Dictionary<string, QueryInternal>();
                InternalQueryMap.Add(InterfaceId, GrainMap);
            }
            GrainMap.Add(Query.GetMethodAndArgsKey(), Query);
        }

        public QueryInternal<T> Get<T>(InvokeMethodRequest request)
        {
            return (QueryInternal<T>)Get(request);
        }

        public QueryInternal Get(InvokeMethodRequest request)
        {
            Dictionary<string, QueryInternal> GrainMap;
            InternalQueryMap.TryGetValue(request.InterfaceId, out GrainMap);
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

        public void Update(InvokeMethodRequest request, object result)
        {
            QueryCache Cache = GetQueryCache(request);
            Type arg_type = result.GetType();
            Type class_type = typeof(QueryCache<>);
            Type class_type2 = class_type.MakeGenericType(new Type[] { arg_type });
            MethodInfo mi = class_type2.GetMethod("TriggerUpdate");
            mi.Invoke(Cache, new object[] { result });
        }

        public async Task<IEnumerable<IEnumerable<Message>>> RecalculateQueries(int interfaceId)
        {
            Dictionary<string, QueryInternal> GrainMap;
            InternalQueryMap.TryGetValue(interfaceId, out GrainMap);
            if (GrainMap == null)
            {
                return Enumerable.Empty<IEnumerable<Message>>();
            }
            var Tasks = GrainMap.Values.Select((q) => q.Recalculate());
            return await Task.WhenAll(Tasks);
        }

        // When new update comes in:
        // resultTask = OrleansTaskExtentions.ConvertTaskViaTcs(resultTask);
        //    return resultTask.Unbox<T>();
    }
}