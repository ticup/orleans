using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    class RcRootSummary<T> : RcSummary<T>
    {

        Guid Guid;
        Func<Task<T>> Computation;
        public ReactiveComputation<T> Rc { get; }

        public RcRootSummary(GrainId grainId, Guid guid, Func<Task<T>> computation, ReactiveComputation<T> rc): base(grainId)
        {
            Guid = guid;
            Computation = computation;
            Rc = rc;
        }

        protected override Task OnChange()
        {
            // Notify the ReactiveComputation that belongs to this Summary
            Rc.OnNext(Result);
            return base.OnChange();
        }


        public override Task<object> Execute()
        {
            return Computation().Box();
        }


        public override string GetLocalKey()
        {
            return GetKey();
        }

        public override string GetKey()
        {
            return Guid.ToString();
        }

        public override string ToString()
        {
            return "RootSummary" + Guid;
        }
    }
}
