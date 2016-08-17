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
    class PushDependency : IDisposable
    {
        DateTime LastKeepAlive;
        int Interval;

        IRcManager Observer;

        System.Threading.Timer Timer;
        public string PushKey { get; private set; }

        public PushDependency(string pushKey, RcSummary summary, IRcManager observer, int interval)
        {
            Observer = observer;
            Interval = interval;
            PushKey = pushKey;
            RefreshKeepAlive();
            Timer = new System.Threading.Timer(_ =>
            {
                var now = DateTime.UtcNow;
                if ((now - LastKeepAlive).TotalMilliseconds > interval * 2)
                {
                    summary.RemovePushDependency(this);
                }
            }, null, 0, interval * 2);
        }

        public void Dispose()
        {
            Timer.Dispose();
        }

        public void Refresh(int interval)
        {
            RefreshKeepAlive();
            if (interval < Interval)
            {
                Interval = interval;
            }
            Timer.Change(0, Interval * 2);
        }

        void RefreshKeepAlive()
        {
            LastKeepAlive = DateTime.UtcNow;
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
}
