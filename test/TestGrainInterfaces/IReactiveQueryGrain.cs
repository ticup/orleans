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
        Task<Query<string>> MyQuery(string someArg);
        Task SetString(string newString);
    }

    public interface IMyOtherReactiveGrain : IGrainWithIntegerKey, IReactiveGrain
    {
        Task<string> OtherMethod();
    }
}
