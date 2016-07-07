using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    class QueryManager
    {
        // Double Map of interfaceId -> "methodId(args)" -> InternalQuery
        Dictionary<int, Dictionary<string, InternalQuery>> InternalQueryMap;

        // TODO: Only for client?
        // Map of query key -> Action that triggers the UpdateTask for a Query
        //Dictionary<string, Query> QueryMap;

        public bool InQuery { get; }

        public QueryManager()
        {
            InternalQueryMap = new Dictionary<int, Dictionary<string, InternalQuery>>();
            //QueryMap = new Dictionary<string, Query>();
            InQuery = false;
        }

        public static string GetKey(int InterfaceId, int MethodId, object[] Arguments)
        {
            return InterfaceId + "." + MethodId + "(" + Utils.EnumerableToString(Arguments) + ")";
        }

        public static string GetMethodAndArgsKey(int methodId, object[] args)
        {
            return methodId +"(" + Utils.EnumerableToString(args) + ")";
        }

        public void Add(InternalQuery Query)
        {
            var InterfaceId = Query.GetInterfaceId();
            Dictionary<string, InternalQuery> GrainMap;
            InternalQueryMap.TryGetValue(InterfaceId, out GrainMap);
            if (GrainMap == null)
            {
                GrainMap = new Dictionary<string, InternalQuery>();
                InternalQueryMap.Add(InterfaceId, GrainMap);
            }
            GrainMap.Add(Query.GetMethodAndArgsKey(), Query);
        }

        public InternalQuery Get(int interfaceId, int methodId, object[] arguments)
        {
            Dictionary<string, InternalQuery> GrainMap;
            InternalQueryMap.TryGetValue(interfaceId, out GrainMap);
            if (GrainMap == null)
            {
                // TODO
                throw new Exception("Got update for non-existent query");
            }
            InternalQuery Query;
            GrainMap.TryGetValue(GetMethodAndArgsKey(methodId, arguments), out Query);
            if (Query == null)
            {
                // TODO
                throw new Exception("Got update for non-existent query");
            }
            return Query;
        }

        //public void Add(Query Query)
        //{
        //    QueryMap.Add(Query.GetKey(), Query);
        //}

        public IEnumerable<Message> Update<T>(int InterfaceId, int MethodId, object[] Arguments, T result)
        {
            InternalQuery<T> Query = (InternalQuery<T>) Get(InterfaceId, MethodId, Arguments);
            return Query.TriggerUpdate(result);
        }

        public IEnumerable<Message> Update(int InterfaceId, int MethodId, object[] Arguments, object result)
        {
            InternalQuery Query = Get(InterfaceId, MethodId, Arguments);
            Type arg_type = result.GetType();
            Type class_type = typeof(InternalQuery<>);
            Type class_type2 = class_type.MakeGenericType(new Type[] { arg_type });
            MethodInfo mi = class_type2.GetMethod("TriggerUpdate");
            IEnumerable<Message> pushMessages = (IEnumerable<Message>)mi.Invoke(Query, new object[] { result });
            return pushMessages;
        }

        public async Task<IEnumerable<IEnumerable<Message>>> RecalculateQueries(int interfaceId)
        {
            Dictionary<string, InternalQuery> GrainMap;
            InternalQueryMap.TryGetValue(interfaceId, out GrainMap);
            var Tasks = GrainMap.Values.Select((q) => q.Recalculate());
            return await Task.WhenAll(Tasks);
        }

        // When new update comes in:
        // resultTask = OrleansTaskExtentions.ConvertTaskViaTcs(resultTask);
        //    return resultTask.Unbox<T>();
    }
}