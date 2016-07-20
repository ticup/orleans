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
        Task<string> MyLayeredComputation();
        Task SetString(string newString);
        Task SetGrains(List<IMyOtherReactiveGrain> grains);
    }

    public interface IMyOtherReactiveGrain : IGrainWithIntegerKey, IReactiveGrain
    {
        Task<string> GetValue();

        Task SetValue(string newValue);
    }

    public interface IReactiveGrainTestsGrain: IGrainWithIntegerKey
    {
        Task OnUpdateAsyncAfterUpdate();
        Task OnUpdateAsyncBeforeUpdate();
        Task OnUpdateAsyncBeforeUpdate2();
        Task DontPropagateWhenNoChange();
        Task MultipleIteratorsSameComputation();
        Task MultiLayeredComputation();
        Task IteratorShouldOnlyReturnLatestValue();

    }
}
