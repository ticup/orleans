using Orleans;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public interface IMyReactiveGrain : IGrainWithIntegerKey, IReactiveGrain
    {
        Task<ReactiveComputation<string>> MyReactiveComp(string someArg);
        Task<ReactiveComputation<string>> MyLayeredComputation();
        Task SetString(string newString);
        Task SetGrains(List<IMyOtherReactiveGrain> grains);
    }

    public interface IMyOtherReactiveGrain : IGrainWithIntegerKey, IReactiveGrain
    {
        Task<string> GetValue();

        Task SetValue(string newValue);
    }
}
