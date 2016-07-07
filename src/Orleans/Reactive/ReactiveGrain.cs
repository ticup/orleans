using Orleans.CodeGeneration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public class ReactiveGrain : Grain
    {
        //public async Task<object> StartRootQuery<T>(InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message)
        //{
        //    var Timeout = (int)RequestContext.Get("QueryTimeout");
        //    //Query<T> Query = new Query<T>("test");
        //    //RuntimeClient.Current.QueryManager.AddQuery()
        //    var result = await invoker.Invoke(this, request);
        //    Query<T> Query = (Query<T>)(result);
        //    InternalQuery<T> InternalQuery = new InternalQuery<T>(request, this, invoker, message, true);
        //    InternalQuery.SetInitialResult(Query.Result);
        //    RuntimeClient.Current.QueryManager.Add(InternalQuery);
        //    return Query.Result;
        //}

        //public async Task<object> Invoke(MethodInfo methodInfo, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout)
        //{
        //    var result = await invoker.Invoke(this, request);
        //}

    }
}
