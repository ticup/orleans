using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    /// <summary>
    /// A grain that wants to expose Reactive Computations needs to implement this interface in its interface instead of the IGrain.
    /// </summary>
    public interface IReactiveGrain : IGrain
    {
    }
}
