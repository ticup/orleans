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
        List<ReactiveComputation<T>> Observers;


        public RcRootSummary(GrainId grainId, Guid guid, Func<Task<T>> computation): base(grainId)
        {
            Guid = guid;
            Computation = computation;
            Observers = new List<ReactiveComputation<T>>();
        }

        public override async Task<object> Calculate()
        {
            var result = await base.Calculate();
            await Notify();
            return result;
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


        public override string GetLocalKey()
        {
            return GetKey();
        }

        public override string GetKey()
        {
            return Guid.ToString();
        }
    }
}
