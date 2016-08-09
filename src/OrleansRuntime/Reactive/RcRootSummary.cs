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

        public override async Task<object> Calculate()
        {
            var result = await base.Calculate();
            await Rc.OnNext(this.Result);            
            return result;
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
