using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public class QueryManager
    {
        Dictionary<string, Query> QueryMap;
        public bool InQuery { get; }

        public QueryManager()
        {
            QueryMap = new Dictionary<string, Query>();
            InQuery = false;
        }
        public void AddQuery(Query Query)
        {
            QueryMap.Add(Query.GetKey(), Query);
        }

        // When new update comes in:
        // resultTask = OrleansTaskExtentions.ConvertTaskViaTcs(resultTask);
        //    return resultTask.Unbox<T>();
    }
}