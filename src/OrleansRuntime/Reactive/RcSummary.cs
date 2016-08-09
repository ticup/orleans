﻿using Orleans.CodeGeneration;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    interface RcSummary : IRcCacheObserverWithKey
    {
        Task<object> Calculate();
        void SetResult(object newResult);
        IEnumerable<Message> GetPushMessages();
        PushDependency GetOrAddPushDependency(ActivationAddress activationAddress, int timeout);
        RcEnumeratorAsync GetDependencyEnum(string FullMethodKey);
        void AddDependencyEnum(string FullMethodKey, RcEnumeratorAsync rcEnum);
        bool HasDependencyOn(string fullMethodKey); 

        Task<object> Execute();

        //string GetMethodAndArgsKey();
        //int GetInterfaceId();
        string GetFullKey();
        string GetLocalKey();
        int GetTimeout();
    }

    class RcSummary<TResult> : RcSummary
    {
        private TResult PrevResult;
        private byte[] PrevSerializedResult;

        public TResult Result { get; private set; }
        private byte[] SerializedResult;

        private TaskCompletionSource<TResult> Tcs;
        public Task<TResult> OnFirstCalculated { get; private set; }

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;
        private Guid ActivationPrimaryKey;
        public GrainId GrainId { get; private set; }

        private InvokeMethodRequest Request;

        private Dictionary<string, PushDependency> PushesTo = new Dictionary<string, PushDependency>();

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
            var key = GetDependentKey(dependentAddress);
            PushesTo.Add(key, new PushDependency(dependentAddress, timeout));
        }

        protected RcSummary(GrainId grainId)
        {
            GrainId = grainId;
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
        }


        public void SetResult(object result)
        {
            SetResult((TResult)result);
        }

        public void SetResult(TResult result)
        {
            PrevResult = Result;
            PrevSerializedResult = SerializedResult;

            Result = result;
            BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
            Serialization.SerializationManager.Serialize(result, stream);
            SerializedResult = stream.ToByteArray();
        }

        // This is called whenever one of the summaries we depend on has its value changed (ignore the result).
        public virtual async Task OnNext(object result)
        {
            await Calculate();
        }


        public IEnumerable<Message> GetPushMessages()
        {
            if (PrevResult == null || !SerializationManager.CompareBytes(PrevSerializedResult, SerializedResult))
            {
                return PushesTo.Values.Select((d) => Message.CreatePushMessage(ActivationPrimaryKey, d.ActivationAddress, Request, Result));
            }
            else
            {
                return new List<Message>();
            }
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

        public PushDependency GetOrAddPushDependency(ActivationAddress dependentAddress, int timeout)
        {
            var Key = GetDependentKey(dependentAddress);
            PushDependency Push;
            PushesTo.TryGetValue(Key, out Push);
            if (Push == null)
            {
                Push = new PushDependency(dependentAddress, timeout);
                PushesTo.Add(Key, Push);
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

        public virtual string GetActivationKey()
        {
            return RcManager.GetFullActivationKey(Request.InterfaceId, ActivationPrimaryKey);
        }

        public virtual string GetLocalKey()
        {
            return GetMethodAndArgsKey();
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
