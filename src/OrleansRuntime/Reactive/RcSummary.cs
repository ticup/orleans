using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    interface RcSummary : IDisposable
    {
        Task<bool> UpdateResult(object newResult, Exception exception);
        byte[] SerializedResult { get; }

        #region Execution
        void EnqueueExecution();
        Task<object> Execute();
        #endregion

        #region Push Dependency Tracking
        IEnumerable<KeyValuePair<SiloAddress, PushDependency>> GetDependentSilos();
        bool AddPushDependency(SiloAddress dependentSilo, int timeout);
        void RemoveDependentSilo(SiloAddress silo);
        #endregion

        #region Identifier Retrieval
        string GetFullKey();
        string GetLocalKey();
        string GetCacheMapKey();
        #endregion
    }

    /// <summary>
    /// Represents the execution of an activation method.
    /// NOTE: we currently assume a RcSummary is not accessed concurrently, because of the RcSummaryWorker.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    class RcSummary<TResult> : RcSummaryBase<TResult>, RcSummary
    {
        InsideRcManager RcManager;

        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstCalculated { get; private set; }

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;
        private object ActivationPrimaryKey;

        private InvokeMethodRequest Request;

        private Dictionary<SiloAddress, PushDependency> PushesTo = new Dictionary<SiloAddress, PushDependency>();

        private int Interval;

        /// <summary>
        /// Represents a particular method invocation (methodId + arguments) for a particular grain activation.
        /// 1) It can observe other RcCaches, when that invocation is used as a sub computation.
        ///    This observation is handled intra-grain and intra-task.
        /// 2) It is observed by a single RcCache (which is shared between grains), which is notified whenever the result of the computation changes.
        ///    This observation is handled inter-grain and inter-task.
        /// </summary>
        public RcSummary(object activationPrimaryKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, ActivationAddress dependentAddress, int timeout, InsideRcManager rcManager) : this(timeout)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            ActivationPrimaryKey = activationPrimaryKey;
            PushesTo.Add(dependentAddress.Silo, new PushDependency(timeout));
            RcManager = rcManager;
        }

        protected RcSummary(int timeout) : base(timeout)
        {
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
        }

       

        

        public override Task<object> Execute()
        {
            var Result = MethodInvoker.Invoke(Target, Request);
            return Result;
        }




        /// <summary>
        /// Gets called whenever the Result is updated and is different from the previous.
        /// </summary>
        public override Task OnChange()
        {
            return Task.WhenAll(GetDependentSilos().ToList().Select((kvp) =>
                PushToSilo(kvp.Key, kvp.Value)));
        }

        /// <summary>
        /// Push the Result to the silo, which has a Cache that depends on this Summary.
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="dependency"></param>
        /// <returns></returns>
        private async Task PushToSilo(SiloAddress silo, PushDependency dependency)
        {
            // get the Rc Manager System Target on the remote grain
            var rcmgr = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, silo);

            bool silo_remains_dependent = false;
            try
            {
                // send a push message to the rc manager on the remote grain
                silo_remains_dependent = await rcmgr.UpdateSummaryResult(GetCacheMapKey(), SerializedResult, ExceptionResult);
            }
            catch (Exception e)
            {
                var GrainId = RuntimeClient.Current.CurrentActivationAddress;
                RuntimeClient.Current.AppLogger.Warn(ErrorCode.ReactiveCaches_PushFailure, "Caught exception when updating summary result for {0} on silo {1}: {2}", GrainId, silo, e);
            }

            if (!silo_remains_dependent)
            {
                RemoveDependentSilo(silo);
            }
        }


        public bool AddPushDependency(SiloAddress dependentSilo, int timeout)
        {
            PushDependency Push;
            bool PushNow = false;
            lock (PushesTo)
            {
                RcSummaryBase Summary;
                RcManager.GetCurrentSummaryMap().TryGetValue(GetLocalKey(), out Summary);
                if (Summary != this)
                {
                    return false;
                }

                PushesTo.TryGetValue(dependentSilo, out Push);
                if (Push == null)
                {
                    Push = new PushDependency(timeout);
                    PushesTo.Add(dependentSilo, Push);
                    PushNow = State != RcSummaryState.NotYetComputed;
                }
            }

            // If we just added this dependency and the summary already has a computed value,
            // we immediately push it to the silo.
            if (PushNow)
            {
                var task = PushToSilo(dependentSilo, Push);
            }
            return true;
        }





        #region Push Dependency Tracking
        public IEnumerable<KeyValuePair<SiloAddress, PushDependency>> GetDependentSilos()
        {
            return PushesTo;
        }

        public void RemoveDependentSilo(SiloAddress silo)
        {
            lock (PushesTo)
            {
                PushesTo.Remove(silo);
                if (PushesTo.Count == 0)
                {
                    RcSummaryBase RcSummary;
                    var success = RcManager.GetCurrentSummaryMap().TryRemove(GetLocalKey(), out RcSummary);
                    if (!success)
                    {
                        throw new OrleansException("illegal state");
                    }
                }
            }
        }
        #endregion

        #region Key Retrieval
        public virtual string GetActivationKey()
        {
            return InsideRcManager.GetFullActivationKey(Request.InterfaceId, ActivationPrimaryKey);
        }

        public override string GetLocalKey()
        {
            return GetMethodAndArgsKey();
        }

        public virtual string GetCacheMapKey()
        {
            return InsideRcManager.MakeCacheMapKey(ActivationPrimaryKey, Request);
        }

        public virtual string GetKey()
        {
            return GetFullKey();
        }

        // To be used for inter-grain identification
        public override string GetFullKey()
        {
            return GetInterfaceId() + "." + GetMethodAndArgsKey();
        }

        public string GetMethodAndArgsKey()
        {
            return InsideRcManager.GetMethodAndArgsKey(Request);
        }

        public int GetInterfaceId()
        {
            return Request.InterfaceId;
        }



        public static string GetDependentKey(ActivationAddress dependentAddress)
        {
            return dependentAddress.ToString();
        }

        #endregion

        public override string ToString()
        {
            return "Summary " + GetCacheMapKey();
        }



    }

}
