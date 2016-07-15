using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{


    interface IQueryCacheObserver
    {
        string GetKey();
        Task OnNext(object result);
    }
}
