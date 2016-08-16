using Orleans.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    class OutsideReactiveScheduler : SingleThreadedScheduler
    {
        public RcRootSummary RcRootSummary { get; private set; }

        public OutsideReactiveScheduler(RcRootSummary rcRootSummary) : base()
        {
            RcRootSummary = rcRootSummary;
        }
    }
}
