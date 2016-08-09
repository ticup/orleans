using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    /// <summary>
    ///  Keeps track of a dependency where an RcSummary pushes to a RcCache
    /// </summary>
    class PushDependency
    {
        DateTime LastKeepAlive;
        int Timeout;

        public Dictionary<int, TimeoutTracker> RcCacheDependencies = new Dictionary<int, TimeoutTracker>();

        public PushDependency(int timeout)
        {
            Timeout = timeout;
        }
    }

    class TimeoutTracker
    {
        public int Timeout;
        public DateTime LastKeepAlive;
        public TimeoutTracker(int timeout)
        {
            Timeout = timeout;
            Refresh();
        }

        public void Refresh()
        {
            LastKeepAlive = DateTime.UtcNow;
        }

        public void Update(int timeout)
        {
            Timeout = timeout;
            Refresh();
        }
    }
}
