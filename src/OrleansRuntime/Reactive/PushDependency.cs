using Orleans.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    /// <summary>
    ///  Keeps track of a dependency where an RcSummary pushes to a RcCache
    /// </summary>
    class PushDependency
    {
        DateTime LastKeepAlive;
        int Timeout;

        IRcManager Observer;

        public PushDependency(IRcManager observer, int timeout)
        {
            Observer = observer;
            Timeout = timeout;
        }

        /// <summary>
        /// Push the Result to the silo, which has a Cache that depends on this Summary.
        /// </summary>
        public void PushResult(string cacheKey, byte[] serializedResult, Exception exceptionResult)
        {
            try
            {
                Observer.UpdateSummaryResult(cacheKey, serializedResult, exceptionResult);
            }
            catch (Exception e)
            {
                var GrainId = RuntimeClient.Current.CurrentActivationAddress;
                RuntimeClient.Current.AppLogger.Warn(ErrorCode.ReactiveCaches_PushFailure, "Caught exception when updating summary result for {0} on node {1}: {2}", GrainId, Observer, e);
            }
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
