using Orleans.CodeGeneration;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    interface QueryCache
    {

    }
    class QueryCache<TResult> : QueryCache
    {
        private InvokeMethodRequest Request;
        private InvokeMethodOptions Options;
        private GrainReference Target;
        private bool IsRoot = false;

        public TResult Result { get; private set; }
        private List<Query<TResult>> Queries = new List<Query<TResult>>();


        // Used to construct an InternalQuery that does not push to others.
        public QueryCache(InvokeMethodRequest request, GrainReference target, bool isRoot)
        {
            Request = request;
            Target = target;
            IsRoot = isRoot;
        }


        public void SetResult(TResult result)
        {
            Result = result;
        }

        public void TriggerUpdate(TResult result)
        {
            SetResult(result);

            foreach (var Query in Queries)
            {
                Query.TriggerUpdate(result);
            }
        }

        public void AddQuery(Query<TResult> Query)
        {
            Queries.Add(Query);
        }
    }
}
