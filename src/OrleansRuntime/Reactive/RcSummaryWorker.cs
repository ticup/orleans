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
            var logger = rcManager.Logger;

            if (Current != null)
            {
                throw new Runtime.OrleansException("illegal state");
            }

            Dictionary<RcSummary, TaskCompletionSource<object>> work;

            logger.Verbose("Worker {0} started", grainId);

            lock (this)
            {
                // take all work out of the queue for processing
                work = queuedwork;
                queuedwork = new Dictionary<RcSummary, TaskCompletionSource<object>>();
            }

            var notificationtasks = new List<Task>();

            foreach (var workitem in work)
            {
                var summary = workitem.Key;
                var Resolver = workitem.Value;

                logger.Verbose("Worker {0} is scheduling summary {1}", grainId, summary);

                var context = RuntimeContext.CurrentActivationContext.CreateReactiveContext();

                object result = null;
                Exception exception_result = null;

                await (RuntimeClient.Current.ExecAsync(async () =>
                {
                    logger.Verbose("Worker {0} starts executing summary {1}", grainId, summary);

                    Current = summary;

                    // Execute the computation
                    try
                    {
                        result = await Current.Execute();
                    }
                    catch (Exception e)
                    {
                        exception_result = e;
                    }

                    Current = null;

                    logger.Verbose("Worker {0} finished executing summary {1}, result={2}, exc={3}", grainId, summary, result, exception_result);

                }, context, "Reactive Computation"));

                // Set the result in the summary
                // todo: propagate exception results the same way as normal results
                var changed = summary.UpdateResult(result);

                // Resolve promise for this work
                Resolver.SetResult(result);

                // If result has changed, notify all caches
                if (changed)
                {
                        foreach (var kvp in summary.GetDependentSilos().ToList())
                            notificationtasks.Add(PushToSilo(summary, kvp.Key, kvp.Value));
                }
            }

            logger.Verbose("Worker {0} waiting for {1} notification tasks", grainId, notificationtasks.Count);

            await Task.WhenAll(notificationtasks);

            logger.Verbose("Worker {0} done", grainId);
        }

        private async Task PushToSilo(RcSummary summary, SiloAddress silo, PushDependency dependency)
        {
            // get the Rc Manager System Target on the remote grain
            var rcmgr = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, silo);

            bool silo_remains_dependent = false;
            try
            {
                // send a push message to the rc manager on the remote grain
                silo_remains_dependent = await rcmgr.UpdateSummaryResult(summary.GetCacheMapKey(), summary.SerializedResult);
            }
            catch (Exception e)
            {
                rcManager.Logger.Warn(ErrorCode.ReactiveCaches_PushFailure, "Caught exception when updating summary result for {0} on silo {1}: {2}", grainId, silo, e);
            }

            if (!silo_remains_dependent)
            {
                summary.RemoveDependentSilo(silo);
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
