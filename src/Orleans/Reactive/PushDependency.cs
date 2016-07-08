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

        DateTime LastKeepAlive;

        public Dictionary<int, TimeoutTracker> QueryDependencies = new Dictionary<int, TimeoutTracker>();

        public PushDependency(int queryId, SiloAddress targetSilo, GrainId targetGrain, ActivationId activationId, int timeout)
        {
            TargetSilo = targetSilo;
            TargetGrain = targetGrain;
            ActivationId = activationId;
            QueryDependencies.Add(queryId, new TimeoutTracker(timeout));
        }

        public void AddQueryDependency(int queryId, int timeout)
        {
            QueryDependencies.Add(queryId, new TimeoutTracker(timeout));
        }
    }

    class TimeoutTracker
    {
        public int Timeout;
        public DateTime LastKeepAlive;
        public TimeoutTracker(int timeout)
        {
            Timeout = timeout;
        }
    }
}
