using Orleans.CodeGeneration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    /// <summary>
    /// A grain that wants to expose Reactive Computations needs to implement this class instead of the Grain class.
    /// </summary>
    public class ReactiveGrain : Grain
    {

    }
}
