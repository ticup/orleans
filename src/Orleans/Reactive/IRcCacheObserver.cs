using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{

    public interface IRcCacheObserver
    {
        Task OnNext(object result);
    }

    public interface IRcCacheObserverWithKey : IRcCacheObserver
    {
        string GetKey();
    }
}
