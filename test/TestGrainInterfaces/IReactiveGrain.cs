using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public interface IReactiveGrain : IGrainWithIntegerKey
    {
        Query<string> MyQuery(string someArg);
        Task SetString(string newString);
    }

    public interface IReactiveOtherGrain : IGrainWithIntegerKey
    {
        [Reactive]
        string OtherMethod();
    }
}