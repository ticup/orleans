using Orleans;
using Orleans.Reactive;
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
        Task<string> GetValue(int offset = 0);

        Task SetValue(string newValue);
        Task<bool> FaultyMethod();

    }

    public interface IReactiveGrainBase : IReactiveGrain
    {
        Task<string> GetValue(int offset = 0);
        Task SetValue(string newValue);
    }

    public interface IReactiveGrainGuidCompoundKey : IGrainWithGuidCompoundKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainGuidKey : IGrainWithGuidKey, IReactiveGrainBase
    { 
    }

    public interface IReactiveGrainIntegerCompoundKey : IGrainWithIntegerCompoundKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainIntegerKey : IGrainWithIntegerKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainStringKey : IGrainWithStringKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainTestsGrain: IGrainWithIntegerKey
    {
        Task OnUpdateAsyncAfterUpdate(int randomoffset);
        Task OnUpdateAsyncBeforeUpdate(int randomoffset);
        Task OnUpdateAsyncBeforeUpdate2(int randomoffset);
        Task DontPropagateWhenNoChange(int randomoffset);
        Task FilterIdenticalResults(int randomoffset);
        Task MultipleIteratorsSameComputation(int randomoffsett);
        Task MultiLayeredComputation(int randomoffset);
        Task IteratorShouldOnlyReturnLatestValue(int randomoffset);
        Task MultipleComputationsUsingSameMethodSameActivation(int randomoffset);
        Task MultipleComputationsUsingSameMethodDifferentActivation(int randomoffset);
        Task MultipleCallsFromSameComputation(int randomoffset);
        Task ExceptionPropagation(int randomoffset);
        Task GrainKeyTypes(int randomoffset);
        Task CacheDependencyInvalidation(int randomoffset);

    }

}
