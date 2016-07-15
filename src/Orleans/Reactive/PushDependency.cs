using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    class PushDependency
    {
        public SiloAddress TargetSilo;
        public GrainId TargetGrain;
        public ActivationId ActivationId;
        public ActivationAddress ActivationAddress;

        DateTime LastKeepAlive;
        int Timeout;

        public Dictionary<int, TimeoutTracker> QueryDependencies = new Dictionary<int, TimeoutTracker>();

        public PushDependency(SiloAddress targetSilo, GrainId targetGrain, ActivationId activationId, ActivationAddress activationAddress, int timeout)
        {
            TargetSilo = targetSilo;
            TargetGrain = targetGrain;
            ActivationId = activationId;
            ActivationAddress = activationAddress;
            Timeout = timeout;
            //QueryDependencies.Add(new TimeoutTracker(timeout));
        }

        //public void AddQueryDependency(int timeout)
        //{
        //    TimeoutTracker tracker;
        //    QueryDependencies.TryGetValue(out tracker);

        //    // This means the user already called .KeepAlive() on the root of this query, just update the information
        //    if (tracker != null)
        //    {
        //        tracker.Update(timeout);
        //    }
        //    QueryDependencies.Add(queryId, new TimeoutTracker(timeout));
        //}
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
