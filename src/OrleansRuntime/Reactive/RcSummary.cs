using Orleans.CodeGeneration;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    interface RcSummary
    {
        Task<object> Calculate();
        Task<bool> UpdateResult(object newResult);
        byte[] SerializedResult { get; }
        IEnumerable<KeyValuePair<SiloAddress, PushDependency>> GetDependentSilos();
        PushDependency GetOrAddPushDependency(SiloAddress silo, int timeout);
        void RemoveDependentSilo(SiloAddress silo);
        RcEnumeratorAsync GetDependencyEnum(string FullMethodKey);
        void AddDependencyEnum(string FullMethodKey, RcEnumeratorAsync rcEnum);
        bool HasDependencyOn(string fullMethodKey);

        Task<object> Execute();

        string GetFullKey();
        string GetLocalKey();
        string GetCacheMapKey();
        int GetTimeout();
    }

    class RcSummary<TResult> : RcSummary
    {
        public TResult Result { get; private set; }
        public byte[] SerializedResult { get; private set; }

        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstCalculated { get; private set; }

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;
        private Guid ActivationPrimaryKey;
        public GrainId GrainId { get; private set; }

        private InvokeMethodRequest Request;

        private Dictionary<SiloAddress, PushDependency> PushesTo = new Dictionary<SiloAddress, PushDependency>();

        Dictionary<string, RcEnumeratorAsync> CacheDependencies = new Dictionary<string, RcEnumeratorAsync>();

        private int Timeout;
        private int Interval;

        /// <summary>
        /// Represents a particular method invocation (methodId + arguments) for a particular grain activation.
        /// 1) It can observe other RcCaches, when that invocation is used as a sub computation.
        ///    This observation is handled intra-grain and intra-task.
        /// 2) It is observed by a single RcCache (which is shared between grains), which is notified whenever the result of the computation changes.
        ///    This observation is handled inter-grain and inter-task.
        /// </summary>
        public RcSummary(GrainId grainId, Guid activationPrimaryKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, ActivationAddress dependentAddress, int timeout) : this(grainId)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            ActivationPrimaryKey = activationPrimaryKey;
            Timeout = timeout;
            PushesTo.Add(dependentAddress.Silo, new PushDependency(timeout));
        }

        protected RcSummary(GrainId grainId)
        {
            GrainId = grainId;
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
        }


        /// <summary>
        /// update the state of the summary.
        /// </summary>
        /// <param name="result">the latest result</param>
        /// <returns>true if the result of the summary changed, or false if it is the same</returns>
        public virtual async Task<bool> UpdateResult(object result)
        {
            var tresult = (TResult)result;
        
            // serialize the result into a byte array
            BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
            Serialization.SerializationManager.Serialize(tresult, stream);
            var serializedresult = stream.ToByteArray();

            if (Result != null && SerializationManager.CompareBytes(SerializedResult, serializedresult))
                return false;

            // store latest result
            Result = tresult;
            SerializedResult = serializedresult;

            await OnChange();

            return true;
        }

        public virtual Task OnChange()
        {
            return Task.WhenAll(GetDependentSilos().ToList().Select((kvp) =>
                PushToSilo(kvp.Key, kvp.Value)));
        }

        private async Task PushToSilo(SiloAddress silo, PushDependency dependency)
        {
            // get the Rc Manager System Target on the remote grain
            var rcmgr = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, silo);

            bool silo_remains_dependent = false;
            try
            {
                // send a push message to the rc manager on the remote grain
                silo_remains_dependent = await rcmgr.UpdateSummaryResult(GetCacheMapKey(), SerializedResult);
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

        // This is called whenever one of the summaries we depend on has its value changed (ignore the result).
        //public virtual async Task OnNext(object result)
        //{
        //    await Calculate();
        //}

        public override string ToString()
        {
            return "Summary"+GetCacheMapKey();
        }

        public Task Start(int timeout, int interval)
        {
            Timeout = timeout;
            Interval = interval;
            return Calculate();
        }

        public virtual Task<object> Calculate()
        {
            return RuntimeClient.Current.EnqueueRcExecution(GrainId, this.GetLocalKey());
        }

        public virtual Task<object> Execute()
        {
            return MethodInvoker.Invoke(Target, Request);
        }

        public PushDependency GetOrAddPushDependency(SiloAddress dependentSilo, int timeout)
        {
            PushDependency Push;
            PushesTo.TryGetValue(dependentSilo, out Push);
            if (Push == null)
            {
                Push = new PushDependency(timeout);
                PushesTo.Add(dependentSilo, Push);
            }
            return Push;
        }


        public bool HasDependencyOn(string fullMethodKey)
        {
            return CacheDependencies.ContainsKey(fullMethodKey);
        }

        public RcEnumeratorAsync GetDependencyEnum(string FullMethodKey)
        {
            RcEnumeratorAsync Result;
            CacheDependencies.TryGetValue(FullMethodKey, out Result);
            return Result;
        }

        public void AddDependencyEnum(string FullMethodKey, RcEnumeratorAsync rcEnum)
        {
            CacheDependencies.Add(FullMethodKey, rcEnum);
        }

        public IEnumerable<KeyValuePair<SiloAddress, PushDependency>> GetDependentSilos()
        {
            return PushesTo;
        }

        public void RemoveDependentSilo(SiloAddress silo)
        {
            PushesTo.Remove(silo);
        }

        public virtual string GetActivationKey()
        {
            return RcManager.GetFullActivationKey(Request.InterfaceId, ActivationPrimaryKey);
        }

        public virtual string GetLocalKey()
        {
            return GetMethodAndArgsKey();
        }

        public virtual string GetCacheMapKey()
        {
            return RcManager.MakeCacheMapKey(ActivationPrimaryKey, Request);
        }

        public virtual string GetKey()
        {
            return GetFullKey();
        }

        // To be used for inter-train identification
        public string GetFullKey()
        {
            return GetInterfaceId() + "." + GetMethodAndArgsKey();
        }

        public string GetMethodAndArgsKey()
        {
            return RcManager.GetMethodAndArgsKey(Request);
        }

        public int GetInterfaceId()
        {
            return Request.InterfaceId;
        }

        public int GetTimeout()
        {
            return Timeout;
        }

        public static string GetDependentKey(ActivationAddress dependentAddress)
        {
            return dependentAddress.ToString();
        }

     
    }

}
