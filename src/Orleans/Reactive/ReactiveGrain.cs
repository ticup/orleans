using Orleans.CodeGeneration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    public class ReactiveGrain : Grain
    {
        public async Task<object> Invoke<T>(MethodInfo methodInfo, InvokeMethodRequest request, IGrainMethodInvoker invoker)
        {

            byte QueryMessage = (byte)RequestContext.Get("QueryMessage");
            if (QueryMessage == 0)
            {
                var result = (T)await invoker.Invoke(this, request);
                return result;
            }

            switch (QueryMessage)
            {
                case 1:
                    var Timeout = (int)RequestContext.Get("QueryTimeout");
                    //Query<T> Query = new Query<T>("test");
                    //RuntimeClient.Current.QueryManager.AddQuery()
                    var result = await invoker.Invoke(this, request);
                    Query<T> Query = (Query<T>)(result);
                    return Query.Result;
                case 2:
                    // polling
                    throw new Exception("Unknown QueryMessage Kind: " + QueryMessage);
                default:
                    throw new Exception("Unknown QueryMessage Kind: " + QueryMessage);
            }
                
            
        }

    }
}
