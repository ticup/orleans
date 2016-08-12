using Orleans.CodeGeneration;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    interface RcSummary
    {
        Task<bool> UpdateResult(object newResult, Exception exception);
        byte[] SerializedResult { get; }
        IEnumerable<KeyValuePair<SiloAddress, PushDependency>> GetDependentSilos();
        Task<PushDependency> GetOrAddPushDependency(SiloAddress silo, int timeout);
        void RemoveDependentSilo(SiloAddress silo);
        RcEnumeratorAsync GetDependencyEnum(string FullMethodKey);
        void AddDependencyEnum(string FullMethodKey, RcEnumeratorAsync rcEnum);
        bool HasDependencyOn(string fullMethodKey);

        Task<object> EnqueueExecution();
        Task<object> Execute();

        string GetFullKey();
        string GetLocalKey();
        string GetCacheMapKey();
        int GetTimeout();
    }

    enum RcSummaryStatus
    {
        NotYetComputed,
        HasResult,
        Exception
    }

    class RcSummary<TResult> : RcSummary
    {
        RcSummaryStatus State;

        public TResult Result { get; private set; }

        public byte[] SerializedResult { get; private set; }

        public Exception ExceptionResult { get; private set; }

        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstCalculated { get; private set; }

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;
        private object ActivationPrimaryKey;

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
        public RcSummary(object activationPrimaryKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, ActivationAddress dependentAddress, int timeout) : this()
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            ActivationPrimaryKey = activationPrimaryKey;
            Timeout = timeout;
            PushesTo.Add(dependentAddress.Silo, new PushDependency(timeout));
            State = RcSummaryStatus.NotYetComputed;
        }

        protected RcSummary()
        {

            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
            State = RcSummaryStatus.NotYetComputed;
        }

        public Task Start(int timeout, int interval)
        {
            Timeout = timeout;
            Interval = interval;
            return EnqueueExecution();
        }

        public Task<object> EnqueueExecution()
        {
            return RuntimeClient.Current.EnqueueRcExecution(this.GetLocalKey());
        }

        public virtual Task<object> Execute()
        {
            return MethodInvoker.Invoke(Target, Request);
        }


        /// <summary>
        /// Update the state of the summary and notify dependents if it is different from the previous state.
        /// </summary>
        /// <param name="result">the latest result, if there is no exception</param>
        /// <param name="exceptionResult">the latest result</param>
        /// <returns>true if the result of the summary changed, or false if it is the same</returns>
        public virtual async Task<bool> UpdateResult(object result, Exception exceptionResult)
        {
            if (exceptionResult == null)
            {
                
                var tresult = (TResult)result;

                // serialize the result into a byte array
                BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
                Serialization.SerializationManager.Serialize(tresult, stream);
                var serializedresult = stream.ToByteArray();

                if (SerializedResult != null && SerializationManager.CompareBytes(SerializedResult, serializedresult))
                    return false;

                // store latest result
                State = RcSummaryStatus.HasResult;
                Result = tresult;
                SerializedResult = serializedresult;
                ExceptionResult = null;
            } else
            {
                State = RcSummaryStatus.Exception;
                Result = default(TResult);
                SerializedResult = null;
                ExceptionResult = exceptionResult;
            }
            
            await OnChange();

            return true;
        }

        /// <summary>
        /// Gets called whenever the Result is updated and is different from the previous.
        /// </summary>
        protected virtual Task OnChange()
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


        public async Task<PushDependency> GetOrAddPushDependency(SiloAddress dependentSilo, int timeout)
        {
            PushDependency Push;
            PushesTo.TryGetValue(dependentSilo, out Push);
            if (Push == null)
            {
                Push = new PushDependency(timeout);
                PushesTo.Add(dependentSilo, Push);
                if (State != RcSummaryStatus.NotYetComputed)
                {
                    await PushToSilo(dependentSilo, Push);
                }
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
        public override string ToString()
        {
            return "Summary " + GetCacheMapKey();
        }



    }

}
