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

        public PushDependency(SiloAddress targetSilo, GrainId targetGrain, ActivationId activationId)
        {
            TargetSilo = targetSilo;
            TargetGrain = targetGrain;
            ActivationId = activationId;
        }
    }
}
