using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Reactive;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    class OutsideRcManager : RcManagerBase, IRcManager
    {

        ConcurrentDictionary<string, OutsideSummaryWorker> WorkerMap;
        ConcurrentDictionary<Guid, Timer> TimerMap;

        private Task<IRcManager> _Reference;
        public Task<IRcManager> Reference {
            get
            {
                if (_Reference == null)
                {
                    _Reference = GrainClient.GrainFactory.CreateObjectReference<IRcManager>(this);
                }
                return _Reference;  
            }
        }

        public OutsideRcManager(MessagingConfiguration config): base(config)
        {
            WorkerMap = new ConcurrentDictionary<string, OutsideSummaryWorker>();
        }

        #region public API

        /// <summary>
        /// Creates a <see cref="IReactiveComputation{T}"/> from given source.
        /// This also creates a <see cref="RcRootSummary{T}"/> that internally represents the computation and its current result.
        /// The <see cref="IReactiveComputation{T}"/> is subscribed to the <see cref="RcRootSummary{T}"/> to be notified whenever its result changes.
        /// </summary>
        /// <typeparam name="T">Type of the result returned by the source</typeparam>
        /// <param name="computation">The actual computation, or source.</param>
        /// <returns></returns>
        public override ReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation, int refresh = 30000)
        {
            var localKey = Guid.NewGuid();
            var rc = new ReactiveComputation<T>(() =>
            {
                OutsideSummaryWorker disposed;
                WorkerMap.TryRemove(localKey.ToString(), out disposed);
            });
            var RcSummary = new RcRootSummary<T>(localKey, computation, rc, 5000);
            var scheduler = new OutsideReactiveScheduler(RcSummary);
            var worker = new OutsideSummaryWorker(RcSummary, this, scheduler);
            WorkerMap.TryAdd(localKey.ToString(), worker);
            RcSummary.EnqueueExecution();
            return rc;
        }

        /// <summary>
        /// Gets the RcSummary that is currently being executed.
        /// </summary>
        /// <remarks>
        /// Assumes an <see cref="RcSummary"/> is currently being executed on the running task (by means of <see cref="IRuntimeClient.EnqueueRcExecution(GrainId, string)"/>) and
        /// consequently that an <see cref="RcSummaryWorker"/> has been created for this activation.
        /// </remarks>
        public override RcSummaryBase CurrentRc()
        {
            var Scheduler = TaskScheduler.Current;
            var RScheduler = Scheduler as OutsideReactiveScheduler;
            if (RScheduler == null)
            {
                throw new OrleansException("Illegal state");
            }
            return RScheduler.RcRootSummary;
        }

        public override void EnqueueRcExecution(string localKey)
        {
            OutsideSummaryWorker Worker;
            if (WorkerMap.TryGetValue(localKey, out Worker))
            {
                Worker.EnqueueSummary();
            }
        }

        #endregion
    }
}