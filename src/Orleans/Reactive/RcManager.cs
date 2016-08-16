using Orleans.Runtime.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Reactive
{
    internal interface RcManager
    {
        RcCache GetCache(string cacheKey);
        void RemoveCache(string cacheKey);
    }
}
