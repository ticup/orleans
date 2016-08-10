﻿using Orleans;
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

    }

}
