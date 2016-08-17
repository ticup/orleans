using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation<T> ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);

    internal class InsideRcManager : RcManagerBase
    {
        public static InsideRcManager CreateRcManager(Silo silo, MessagingConfiguration config)
        {
            return new InsideRcManager(silo, config);
        }

        private Silo silo;
        public IRcManager Reference { get; private set; }

        bool IsPropagator;

        public InsideRcManager(Silo silo, MessagingConfiguration config): base(config)
        {
            Reference = new InsideRcManagerSystem(this, silo);
            this.silo = silo;
            IsPropagator = false;
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
        public override ReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();
            var SummaryMap = GetCurrentSummaryMap();
            var rc = new ReactiveComputation<T>(() => {
                RcSummaryBase disposed;
                SummaryMap.TryRemove(localKey.ToString(), out disposed);
                disposed.Dispose();
            });
            var RcSummary = new RcRootSummary<T>(localKey, computation, rc);
            var success = SummaryMap.TryAdd(localKey.ToString(), RcSummary);
            if (!success)
            {
                throw new OrleansException("Illegal State");
            }
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
            var Worker = GetCurrentWorker();
            if (Worker == null)
            {
                throw new Runtime.OrleansException("illegal state");
            }
            return Worker.Current;
        }
        

        /// <summary>
        /// Gets the <see cref="RcSummaryWorker"/> for a given grain activation,
        /// if it doesn't exists yet it will be created.
        /// </summary>
        public RcSummaryWorker GetCurrentWorker()
        {
            return ((ActivationData)RuntimeClient.Current.CurrentActivationData).RcSummaryWorker;
        }
        #endregion


        #region Summary API
        public ConcurrentDictionary<string, RcSummaryBase> GetCurrentSummaryMap()
        {
            return ((ActivationData)RuntimeClient.Current.CurrentActivationData).RcSummaryMap;
        }

        /// <summary>
        /// Reschedules calculation of the reactive computations of the current activation.
        /// </summary>
        public void RecomputeSummaries()
        {
            if (IsPropagator)
            {
                foreach (var q in GetCurrentSummaryMap().Values)
                {
                    q.EnqueueExecution();
                }
            }
            
        }

        /// <summary>
        /// Gets the <see cref="RcSummary"/> identified by the local key and current activation
        /// </summary>
        /// <param name="localKey"><see cref="RcSummary.GetLocalKey()"/></param>
        /// <returns></returns>
        public RcSummaryBase GetSummary(string localKey)
        {
            RcSummaryBase RcSummary;
            var SummaryMap = GetCurrentSummaryMap();
            SummaryMap.TryGetValue(localKey, out RcSummary);
            return RcSummary;
        }


        /// <summary>
        /// Concurrently gets or creates a <see cref="RcSummary"/> for given activation and request.
        /// </summary>
        public void CreateAndStartSummary<T>(object activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, Message message)
        {
            RcSummaryBase RcSummary;
            var SummaryMap = GetCurrentSummaryMap();
            var SummaryKey = GetMethodAndArgsKey(request);
            var Timeout = Config.ReactiveComputationRefresh.Multiply(2);
            var NewRcSummary = new RcSummary<T>(activationKey, request, target, invoker, Timeout, this);

            var threadSafeRetrieval = false;
            bool existed;

            // We need to retrieve the summary and add the dependency under the lock of the summary
            // in order to prevent interleaving with removal of the summary from the SummaryMap.
            do
            {
                RcSummary = SummaryMap.GetOrAdd(SummaryKey, NewRcSummary);
                existed = RcSummary != NewRcSummary;

                threadSafeRetrieval = ((RcSummary)RcSummary).AddPushDependency(message);
            } while (!threadSafeRetrieval);
            
            if (!existed)
            {
                NewRcSummary.Initialize();
                RcSummary.EnqueueExecution();
            }
            IsPropagator = true;
        }

        public override void EnqueueRcExecution(string summaryKey)
        {
            var Worker = GetCurrentWorker();
            var Summary = GetSummary(summaryKey);
            if (Summary == null)
            {
                throw new Runtime.OrleansException("illegal state");
            }
            Worker.EnqueueSummary(Summary);
        }

        #endregion
    }
}