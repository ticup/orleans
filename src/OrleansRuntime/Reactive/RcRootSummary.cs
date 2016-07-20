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
        RcSource<Task<T>> Computation;
        List<ReactiveComputation<T>> Observers;


        public RcRootSummary(Guid guid, RcSource<Task<T>> computation)
        {
            Guid = guid;
            Computation = computation;
            Observers = new List<ReactiveComputation<T>>();
        }

        public override async Task OnNext(object result)
        {
            await Recalculate();
            await Notify();
        }

        public Task Notify()
        {
            return Task.WhenAll(Observers.Select(o => o.OnNext(this.Result)));
        }

        public void Subscribe(ReactiveComputation<T> rc)
        {
            Observers.Add(rc);
        }

        public override Task<object> Execute()
        {
            return Computation().Box();
        }

        public override string GetKey()
        {
            return Guid.ToString();
        }

    }
}
