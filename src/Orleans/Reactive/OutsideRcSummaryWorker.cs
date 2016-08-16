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
    class OutsideSummaryWorker : BatchWorker
    {

        Dictionary<RcSummaryBase, bool> queuedwork;

        GrainId GrainId;
        OutsideRcManager RcManager;
        OutsideReactiveScheduler Scheduler;
        RcSummaryBase RcSummary;

        public OutsideSummaryWorker(RcSummaryBase rcSummary, OutsideRcManager rcManager, OutsideReactiveScheduler scheduler)
        {
            RcSummary = rcSummary;
            RcManager = rcManager;
        }

        protected override async Task Work()
        {
            var logger = RcManager.Logger;
            logger.Verbose("Worker started");

            object result = null;
            Exception exception_result = null;

            logger.Verbose("Worker starts executing summary {0}", RcSummary);

            // Execute the computation
            try
            {
                RcSummary.ResetDependencies();
                result = await RcSummary.Execute();
            }
            catch (Exception e)
            {
                exception_result = e;
            }
            try
            {
                RcSummary.CleanupInvalidDependencies();
            }
            catch (Exception e)
            {
                logger.Verbose(e.ToString());
            }

            logger.Verbose("Worker finished executing summary {0}, result={1}, exc={2}", RcSummary, result, exception_result);

            // Set the result/exception in the summary and notify dependents
            await RcSummary.UpdateResult(result, exception_result);

            logger.Verbose("Worker {0} done", GrainId);
        }



        public void EnqueueSummary()
        {
            var task = new Task(() =>
                Notify()
            );
            task.Start(Scheduler);
        }
    }
}
