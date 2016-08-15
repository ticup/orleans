﻿using Orleans.CodeGeneration;
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

        #region Cache Dependency Tracking
        /// <summary>
        /// Returns true if this summary has a dependency on given summary.
        /// </summary>
        /// <param name="FullMethodKey">A <see cref="RcSummary.GetFullKey()"/> that Identifies a <see cref="RcSummary"/></param>
        /// <returns></returns>
        bool HasDependencyOn(string FullMethodKey);

        void KeepDependencyAlive(string fullMethodKey);


        /// <summary>
        /// Sets all the <see cref="RcCacheDependency"/> their <see cref="RcCacheDependency.IsAlive"/> flag to false.
        /// By executing the summary afterwards, all the dependencies that this summary re-used
        /// will have their IsAlive flag set to true again via <see cref="KeepDependencyAlive(string)"/>.
        /// Synergizes with <see cref="CleanupInvalidDependencies"/> in <see cref="RcSummaryWorker.Work"/>
        /// </summary>
        void ResetDependencies();

        /// <summary>
        /// Remove all the <see cref="RcCacheDependency"/> on which this summary no longer depends by looking at the <see cref="RcCacheDependency.IsAlive"/> flag.
        /// Synergizes with <see cref="ResetDependencies"/> in <see cref="RcSummaryWorker.Work"/>
        /// </summary>
        void CleanupInvalidDependencies();

        /// <summary>
        /// Add given cache as a dependency for this summary.
        /// </summary>
        /// <param name="fullMethodKey">The <see cref="GetFullKey"/> that identifies the summary of the <param name="rcCache"></param>
        void AddCacheDependency(string FullMethodKey, RcCache rcCache);
        #endregion

        #region Identifier Retrieval
        string GetFullKey();
        string GetLocalKey();
        string GetCacheMapKey();
        #endregion

        int GetTimeout();
    }

    enum RcSummaryState
    {
        NotYetComputed,
        HasResult,
        Exception
    }

    class RcCacheDependency
    {
        public RcCache Cache;
        public bool IsAlive;

        public RcCacheDependency(RcCache cache)
        {
            Cache = cache;
            IsAlive = true;
        }
    }


    /// <summary>
    /// Represents the execution of an activation method.
    /// NOTE: we currently assume a RcSummary is not accessed concurrently, because of the RcSummaryWorker.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    class RcSummary<TResult> : RcSummary
    {
        RcManager RcManager;
        RcSummaryState State;

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

        private Dictionary<string, RcCacheDependency> CacheDependencies = new Dictionary<string, RcCacheDependency>();

        private int Timeout;
        private int Interval;

        /// <summary>
        /// Represents a particular method invocation (methodId + arguments) for a particular grain activation.
        /// 1) It can observe other RcCaches, when that invocation is used as a sub computation.
        ///    This observation is handled intra-grain and intra-task.
        /// 2) It is observed by a single RcCache (which is shared between grains), which is notified whenever the result of the computation changes.
        ///    This observation is handled inter-grain and inter-task.
        /// </summary>
        public RcSummary(object activationPrimaryKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, ActivationAddress dependentAddress, int timeout, RcManager rcManager) : this(rcManager)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            ActivationPrimaryKey = activationPrimaryKey;
            Timeout = timeout;
            PushesTo.Add(dependentAddress.Silo, new PushDependency(timeout));
            State = RcSummaryState.NotYetComputed;
        }

        protected RcSummary(RcManager rcManager)
        {

            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
            State = RcSummaryState.NotYetComputed;
            RcManager = rcManager;
        }

        public void Start(int timeout, int interval)
        {
            Timeout = timeout;
            Interval = interval;
            EnqueueExecution();
        }

        public void Dispose()
        {
            lock (this)
            {
                if (this.PushesTo.Count > 0)
                {
                    throw new OrleansException("Illegal state");
                }

                var dependencies = CacheDependencies;
                CacheDependencies = new Dictionary<string, RcCacheDependency>();

                foreach(var cd in dependencies)
                {
                    cd.Value.Cache.RemoveDependencyFor(this);
                }
            }
        }

        public void EnqueueExecution()
        {
            RuntimeClient.Current.EnqueueRcExecution(this.GetLocalKey());
        }

        public virtual Task<object> Execute()
        {
            var Result = MethodInvoker.Invoke(Target, Request);
            return Result;
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
                State = RcSummaryState.HasResult;
                Result = tresult;
                SerializedResult = serializedresult;
                ExceptionResult = null;
            } else
            {
                State = RcSummaryState.Exception;
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


        public bool AddPushDependency(SiloAddress dependentSilo, int timeout)
        {
            PushDependency Push;
            bool PushNow = false;
            lock (PushesTo)
            {
                RcSummary Summary;
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



        #region Cache Dependency Tracking
        public bool HasDependencyOn(string fullMethodKey)
        {
            return CacheDependencies.ContainsKey(fullMethodKey);
        }

        public void KeepDependencyAlive(string fullMethodKey)
        {
            RcCacheDependency Dep;
            CacheDependencies.TryGetValue(fullMethodKey, out Dep);
            if (Dep == null)
            {
                throw new OrleansException("illegal state");
            }
            Dep.IsAlive = true;
        }

        public void AddCacheDependency(string FullMethodKey, RcCache rcCache)
        {
            CacheDependencies.Add(FullMethodKey, new RcCacheDependency(rcCache));
        }

        public void ResetDependencies()
        {
            foreach(var dep in CacheDependencies)
            {
                dep.Value.IsAlive = false;
            }
        }

        public void CleanupInvalidDependencies()
        {
            var ToRemove = CacheDependencies.Where((kvp) => !kvp.Value.IsAlive).ToList();
            foreach(var kvp in ToRemove)
            {
                CacheDependencies.Remove(kvp.Key);
                kvp.Value.Cache.RemoveDependencyFor(this);
            }
        }

        #endregion



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
                    RcSummary RcSummary;
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

        // To be used for inter-grain identification
        public virtual string GetFullKey()
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

        #endregion

        public override string ToString()
        {
            return "Summary " + GetCacheMapKey();
        }



    }

}
