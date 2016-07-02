using System.Threading.Tasks;

namespace UnitTests.Grains
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;

    using Orleans;
    using Orleans.CodeGeneration;

    using UnitTests.GrainInterfaces;
    public class ReactiveGrain : Grain, IReactiveGrain, IGrainInvokeInterceptor
    {
        OtherReactiveGrain[] Grains;
        string MyString = "foo";

        public async Task<object> Invoke(MethodInfo methodInfo, InvokeMethodRequest request, IGrainMethodInvoker invoker)
        {

            //Console.WriteLine("Intercepting method invocation on receiving side");
            var result = await invoker.Invoke(this, request);
            return result;
        }


        public Query<string> MyQuery(string someArg)
        {
            //var Strings = this.Grains.Select((g) => g.OtherMethod());
            //return Query<string>.FromResult(Strings.Aggregate((g, s) => g + s));
            return Query<string>.FromResult(MyString);
        }

        public Query<string> MyBiggerQuery(string someArg)
        {
            var Strings = this.Grains.Select((g) => g.OtherMethod());
            return Query<string>.FromResult(Strings.Aggregate((g, s) => g + s));
            //return Query<string>.FromResult(MyString);
        }

        public Task SetString(string newString)
        {
            MyString = newString;
            var Strings = this.Grains.Select((g) => g.OtherMethod());
            return TaskDone.Done;
        }

        public string OtherMethod()
        {
            return "foo";
        }
    }


    public class OtherReactiveGrain : Grain, IReactiveOtherGrain
    {
        string MyString = "foo";

        public Query<string> OtherMethodQuery()
        {
            return Query<string>.FromResult(OtherMethod());
        }

        public string OtherMethod()
        {
            return MyString;
        }
    }
}