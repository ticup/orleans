using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    /// <summary>
    /// Schedules execution of <see cref="RcSummary"/>.
    /// Each activation has a worker, such that only one RcSummary is executed per activation.
    /// </summary>
    /// <remarks>
    /// The workers are currently managed in the <see cref="RcManager.WorkerMap"/> per activation.
    /// They are created on a per-request basis.
    /// </remarks>
    class RcSummaryWorker : BatchWorker
    {

        Dictionary<RcSummary, TaskCompletionSource<object>> queuedwork;


        GrainId grainId;
        RcManager rcManager;

        public RcSummary Current { get; private set; }

        public RcSummaryWorker(GrainId grainId, RcManager rcManager)
        {
            this.grainId = grainId;
            this.rcManager = rcManager;
            queuedwork = new Dictionary<RcSummary, TaskCompletionSource<object>>();
        }

        protected override async Task Work()
        {
            if (Current != null)
            {
                throw new Runtime.OrleansException("illegal state");
            }

            Dictionary<RcSummary, TaskCompletionSource<object>> work;

            lock (this)
            {
                // take all work out of the queue for processing
                work = queuedwork;
                queuedwork = new Dictionary<RcSummary, TaskCompletionSource<object>>();
            }

            foreach (var workitem in work)
            {
                Current = workitem.Key;
                var Resolver = workitem.Value;

                var context = RuntimeContext.CurrentActivationContext.CreateReactive();

                await RuntimeClient.Current.ExecAsync(async () =>
                {

                    // Execute the computation
                    object result = null;
                    Exception exception_result = null;
                    try
                    {
                        result = await Current.Execute();
                    }
                    catch (Exception e)
                    {
                        exception_result = e;
                    }
                    // todo: propagate exception results the same way as normal results

                    // Set the result in the summary
                    var changed = Current.UpdateResult(result);

                    // Resolve promise for this work
                    Resolver.SetResult(result);

                    // If result has changed, notify all caches
                    if (changed)
                    {
                        var tasks = new List<Task>();
                        foreach (var kvp in Current.GetDependentSilos().ToList())
                            tasks.Add(PushToSilo(kvp.Key, kvp.Value));
                        await Task.WhenAll(tasks);
                    }

                    Current = null;

                }, context, "Reactive Computation");
            }
        }

        private async Task PushToSilo(SiloAddress silo, PushDependency dependency)
        {
            // get the Rc Manager System Target on the remote grain
            var rcmgr = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, silo);

            bool silo_remains_dependent = false;
            try
            {
                // send a push message to the rc manager on the remote grain
                silo_remains_dependent = await rcmgr.UpdateSummaryResult(Current.GetCacheMapKey(), Current.SerializedResult);
            }
            catch (Exception e)
            {
                rcManager.Logger.Warn(ErrorCode.ReactiveCaches_PushFailure, "Caught exception when updating summary result for {0} on silo {1}: {2}", grainId, silo, e);
            }

            if (!silo_remains_dependent)
            {
                Current.RemoveDependentSilo(silo);
            }
        }

        public Task<object> EnqueueSummary(RcSummary summary)
        {
            TaskCompletionSource<object> resolver;

            lock (this)
            {
                if (!queuedwork.TryGetValue(summary, out resolver))
                {
                    resolver = new TaskCompletionSource<object>();
                    queuedwork.Add(summary, resolver);
                    Notify();
                }
            }

            return resolver.Task;
        }

    }
}
