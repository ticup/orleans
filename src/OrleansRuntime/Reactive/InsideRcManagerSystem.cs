using Orleans.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    class InsideRcManagerSystem: SystemTarget, IRcManager
    {
        InsideRcManager InsideRcManager;

        internal InsideRcManagerSystem(InsideRcManager insideRcManager, Silo silo) : base(Constants.ReactiveCacheManagerId, silo.SiloAddress)
        {
            InsideRcManager = insideRcManager;
        }

        public Task UpdateSummaryResult(string cacheMapKey, byte[] result, Exception exception)
        {
            return InsideRcManager.UpdateSummaryResult(cacheMapKey, result, exception);
        }

    }
}
