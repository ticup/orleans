using Orleans.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    class RcRootSummary<T>: RcSummaryBase<T>, RcRootSummary
    {

        Guid Guid;
        Func<Task<T>> Computation;
        public ReactiveComputation<T> Rc { get; private set; }

        public RcRootSummary(Guid guid, Func<Task<T>> computation, ReactiveComputation<T> rc, int timeout) : base(timeout)
        {
            Guid = guid;
            Computation = computation;
            Rc = rc;
        }

        public override Task OnChange()
        {
            // Notify the ReactiveComputation that belongs to this Summary
            Rc.OnNext(Result, ExceptionResult);
            return TaskDone.Done;
        }


        public override Task<object> Execute()
        {
            return Computation().Box();
        }


        public override string GetLocalKey()
        {
            return GetKey();
        }

        public override string GetFullKey()
        {
            return GetKey();
        }

        public string GetKey()
        {
            return Guid.ToString();
        }

        public override string ToString()
        {
            return "RootSummary" + Guid;
        }
    }
}
