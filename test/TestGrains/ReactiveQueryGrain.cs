using Orleans;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    public class MyReactiveGrain : ReactiveGrain, IMyReactiveGrain
    {
        List<IMyOtherReactiveGrain> Grains = new List<IMyOtherReactiveGrain>();
        string MyString = "foo";

        public async Task<Query<string>> MyQuery(string someArg)
        {

            return Query<string>.FromResult(MyString);
            //var Tasks = this.Grains.Select((g) => g.OtherMethod());
            //var Strings = await Task.WhenAll(Tasks);
            //return Query<string>.FromResult(Strings.Aggregate((g, s) => g + s));
        }

        public Task SetString(string newString)
        {
            MyString = newString;
            return TaskDone.Done;
        }

    }


    public class ReactiveOtherGrain : ReactiveGrain, IMyOtherReactiveGrain
    {
        string MyString = "foo";


        public async Task<string> OtherMethod()
        {
            return MyString;
        }
    }
}
