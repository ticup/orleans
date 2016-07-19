using Microsoft.VisualStudio.TestTools.UnitTesting;
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

    // Debug log with:  cat *.log | grep -E '(INFO)' | grep -E '(InsideRuntimeClient|GrainReference)'

    public class MyReactiveGrain : ReactiveGrain, IMyReactiveGrain
    {
        List<IMyOtherReactiveGrain> Grains = new List<IMyOtherReactiveGrain>();
        string MyString = "foo";

        public Task<string> MyComp(string someArg)
        {
            return Task.FromResult(MyString);
        }

        public Task SetString(string newString)
        {
            MyString = newString;
            return TaskDone.Done;
        }



        public Task SetGrains(List<IMyOtherReactiveGrain> grains) {
            Grains = grains;
            return TaskDone.Done;
        }

        public async Task<string> MyLayeredComputation()
        {
            var Tasks = this.Grains.Select((g) => g.GetValue());
            var Strings = await Task.WhenAll(Tasks);
            return string.Join(" ", Strings);
        }
       

    }


    public class MyOtherReactiveGrain : ReactiveGrain, IMyOtherReactiveGrain
    {
        string MyString = "foo";


        public Task<string> GetValue()
        {
            return Task.FromResult(MyString);
        }

        public Task SetValue(string newValue)
        {
            MyString = newValue;
            return TaskDone.Done;
        }


    }
}
