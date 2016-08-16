using Orleans.Reactive;
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
    /// The workers are currently managed in the <see cref="InsideRcManager.WorkerMap"/> per activation.
    /// They are created on a per-request basis.
    /// </remarks>
    class RcSummaryWorker : BatchWorker
    {

        Dictionary<RcSummaryBase, bool> queuedwork;

        GrainId GrainId;
        InsideRcManager RcManager;
        ISchedulingContext Context;

        public RcSummaryBase Current { get; private set; }

        public RcSummaryWorker(GrainId grainId, InsideRcManager rcManager, ISchedulingContext context)
        {
            GrainId = grainId;
            RcManager = rcManager;
            Context = context.CreateReactiveContext();
            queuedwork = new Dictionary<RcSummaryBase, bool>();
        }

        protected override async Task Work()
        {
            var logger = RcManager.Logger;
            var notificationtasks = new List<Task>();

            if (Current != null)
            {
                throw new Runtime.OrleansException("illegal state");
            }

            Dictionary<RcSummaryBase, bool> work;

            logger.Verbose("Worker {0} started", GrainId);

            lock (this)
            {
                // take all work out of the queue for processing
                work = queuedwork;
                queuedwork = new Dictionary<RcSummaryBase, bool>();
            }


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
                try
                {
                    Current.CleanupInvalidDependencies();
                } catch (Exception e)
                {
                    logger.Verbose(e.ToString());
                }
                Current = null;

                logger.Verbose("Worker {0} finished executing summary {1}, result={2}, exc={3}", GrainId, summary, result, exception_result);

                // Set the result/exception in the summary and notify dependents
                notificationtasks.Add(summary.UpdateResult(result, exception_result));
            }

            logger.Verbose("Worker {0} waiting for {1} notification tasks", GrainId, notificationtasks.Count);

            // TODO: we could batch notifications to same silo here
            await Task.WhenAll(notificationtasks);

            logger.Verbose("Worker {0} done", GrainId);
        }



        public void EnqueueSummary(RcSummaryBase summary)
        {
            bool Bool;
            RuntimeClient.Current.ExecAction(() =>
            {
                lock (this)
                {
                    if (!queuedwork.TryGetValue(summary, out Bool))
                    {
                        queuedwork.Add(summary, true);
                        Notify();
                    }
                }
            }, Context);
        }
    }
}
