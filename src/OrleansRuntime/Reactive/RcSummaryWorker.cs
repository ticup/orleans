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

        Dictionary<RcSummary, bool> queuedwork;

        GrainId GrainId;
        RcManager RcManager;
        ISchedulingContext Context;

        public RcSummary Current { get; private set; }

        public RcSummaryWorker(GrainId grainId, RcManager rcManager, ISchedulingContext context)
        {
            GrainId = grainId;
            RcManager = rcManager;
            Context = context.CreateReactiveContext();
            queuedwork = new Dictionary<RcSummary, bool>();
        }

        protected override async Task Work()
        {
            var logger = RcManager.Logger;
            var notificationtasks = new List<Task>();

            if (Current != null)
            {
                throw new Runtime.OrleansException("illegal state");
            }

            Dictionary<RcSummary, bool> work;

            logger.Verbose("Worker {0} started", GrainId);

            lock (this)
            {
                // take all work out of the queue for processing
                work = queuedwork;
                queuedwork = new Dictionary<RcSummary, bool>();
            }

            await (RuntimeClient.Current.ExecAsync(async () =>
            {
                foreach (var workitem in work)
                {
                    var summary = workitem.Key;

                    logger.Verbose("Worker {0} is scheduling summary {1}", GrainId, summary);

                    //var context = RuntimeContext.CurrentActivationContext.CreateReactiveContext();

                    object result = null;
                    Exception exception_result = null;


                    logger.Verbose("Worker {0} starts executing summary {1}", GrainId, summary);

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

                    logger.Verbose("Worker {0} finished executing summary {1}, result={2}, exc={3}", GrainId, summary, result, exception_result);

                    // Set the result/exception in the summary and notify dependents
                    notificationtasks.Add(summary.UpdateResult(result, exception_result));
                }
            }, Context, "Reactive Computation"));

            logger.Verbose("Worker {0} waiting for {1} notification tasks", GrainId, notificationtasks.Count);

            // TODO: we could batch notifications to same silo here
            await Task.WhenAll(notificationtasks);

            logger.Verbose("Worker {0} done", GrainId);
        }



        public void EnqueueSummary(RcSummary summary)
        {
            bool Bool;

            lock (this)
            {
                if (!queuedwork.TryGetValue(summary, out Bool))
                {
                    queuedwork.Add(summary, true);
                    Notify();
                }
            }
        }

    }
}
