using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
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
            var notificationtasks = new List<Task>();

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
                        Current.ResetDependencies();
                        result = await Current.Execute();

                    }
                    catch (Exception e)
                    {
                        exception_result = e;
                    }
                    Current.CleanupInvalidDependencies();
                    Current = null;

                    logger.Verbose("Worker {0} finished executing summary {1}, result={2}, exc={3}", grainId, summary, result, exception_result);

                }, context, "Reactive Computation"));

                // Set the result/exception in the summary and notify dependents
                notificationtasks.Add(summary.UpdateResult(result, exception_result));

                // Resolve promise for this work (we don't have to set the exception)
                Resolver.SetResult(result);
                
            }

            logger.Verbose("Worker {0} waiting for {1} notification tasks", grainId, notificationtasks.Count);

            // TODO: we could batch notifications to same silo here!!
            await Task.WhenAll(notificationtasks);

            logger.Verbose("Worker {0} done", grainId);
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
