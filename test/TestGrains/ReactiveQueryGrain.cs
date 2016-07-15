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

        public async Task<ReactiveComputation<string>> MyReactiveComp(string someArg)
        {
            return ReactiveComputation<string>.FromResult(MyString);
        }


        public Task SetGrains(List<IMyOtherReactiveGrain> grains) {
            Grains = grains;
            return TaskDone.Done;
        }

        [Reactive]
        public async Task<ReactiveComputation<string>> MyLayeredComputation()
        {
            var Tasks = this.Grains.Select((g) => g.GetValue());
            var Strings = await Task.WhenAll(Tasks);
            return ReactiveComputation<string>.FromResult(string.Join(" ", Strings));
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


        public async Task<string> GetValue()
        {
            return MyString;
        }

        public Task SetValue(string newValue)
        {
            MyString = newValue;
            return TaskDone.Done;
        }


    }
}
