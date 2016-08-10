using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{

    /// <summary>
    /// Interface for a class that observes new results of the Summary Cache.
    /// </summary>
    public interface IRcCacheObserver
    {
        Task OnNext(object result, Exception exception = null);
    }

    public interface IRcCacheObserverWithKey : IRcCacheObserver
    {
        string GetKey();
    }
}
