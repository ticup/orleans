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
        IEnumerable<KeyValuePair<string, PushDependency>> GetPushDependencies();
        bool AddPushDependency(Message message, bool refresh);
        void RemovePushDependency(PushDependency dep);
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

        private Dictionary<string, PushDependency> PushesTo = new Dictionary<string, PushDependency>();

        private TimeSpan Timeout;
        IDisposable Timer;


        /// <summary>
        /// Represents a particular method invocation (methodId + arguments) for a particular grain activation.
        /// 1) It can observe other RcCaches, when that invocation is used as a sub computation.
        ///    This observation is handled intra-grain and intra-task.
        /// 2) It is observed by a single RcCache (which is shared between grains), which is notified whenever the result of the computation changes.
        ///    This observation is handled inter-grain and inter-task.
        /// </summary>
        public RcSummary(object activationPrimaryKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, TimeSpan timeout, InsideRcManager rcManager)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            ActivationPrimaryKey = activationPrimaryKey;
            RcManager = rcManager;
            Timeout = timeout;
            Tcs = new TaskCompletionSource<TResult>();
            OnFirstCalculated = Tcs.Task;
        }

        public void Initialize()
        {
            if (Timer != null)
            {
                throw new OrleansException("Can only call initialize once");
            }
            Timer = RuntimeClient.Current.CurrentActivationData.RegisterTimer(_ =>
            {
                this.CleanupSubscriptions();
                return TaskDone.Done;
            }, null, Timeout, Timeout);
        }

        public override void Dispose()
        {
            Timer.Dispose();
            base.Dispose();
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
            foreach (var kvp in GetPushDependencies()) {
                kvp.Value.PushResult(GetCacheMapKey(), SerializedResult, ExceptionResult);
            }
            return TaskDone.Done;
        }

        public bool AddPushDependency(Message message, bool refresh)
        {
            PushDependency Push;
            SiloAddress Silo = message.SendingAddress.Silo;
            IRcManager Client = message.RcClientObject;

            var PushKey = Client != null ? GetPushKey(Client) : GetPushKey(Silo);

            lock (PushesTo)
            {
                RcSummaryBase Summary;
                RcManager.GetCurrentSummaryMap().TryGetValue(GetLocalKey(), out Summary);
                if (Summary != this)
                {
                    return false;
                }

                PushesTo.TryGetValue(PushKey, out Push);
                if (Push == null)
                {
                    IRcManager Observer;
                    if (Client != null)
                    {
                        Observer = Client;
                    } else
                    {
                        Observer = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, Silo);
                    }
                    Push = new PushDependency(PushKey, this, Observer, Timeout);
                    PushesTo.Add(PushKey, Push);
                }
            }

            RcManager.Logger.Verbose("Refreshing subscription of {0} for {1}", Push, this);
            Push.KeepAlive();

            // If we already have a result for this summary and it was the silo's initial request
            // for this for summary, push the result to the silo.
            // Note: It's important that the silo lets us know whether its his first call for this summary or not,
            // because the info we have here about the silo might be stale!
            if (State != RcSummaryState.NotYetComputed && !refresh)
            {
                Push.PushResult(GetCacheMapKey(), SerializedResult, ExceptionResult);
            }
            return true;
        }





        #region Push Dependency Tracking
        public IEnumerable<KeyValuePair<string, PushDependency>> GetPushDependencies()
        {
            return PushesTo;
        }

        public void RemovePushDependency(PushDependency dep)
        {
            RcManager.Logger.Verbose("Removing Push Dependency {0} from Summary {1}", dep, this);
            lock (PushesTo)
            {
                PushesTo.Remove(dep.PushKey);
                if (PushesTo.Count == 0)
                {
                    RcSummaryBase RcSummary;
                    var success = RcManager.GetCurrentSummaryMap().TryRemove(GetLocalKey(), out RcSummary);
                    RcManager.Logger.Verbose("Removed Summary {0}", this);
                    RcSummary.Dispose();
                    if (!success)
                    {
                        throw new OrleansException("illegal state");
                    }
                }
            }
        }

        public void CleanupSubscriptions()
        {
            RcManager.Logger.Verbose("Checking Subscription validity for {0} links on {1}", GetPushDependencies().Count(), this);
            var Now = DateTime.UtcNow;
            var ToRemove = new List<PushDependency>();
            lock (PushesTo)
            {
                foreach(var Kvp in GetPushDependencies())
                {
                    RcManager.Logger.Verbose("Checking Subscription validity for {0} : Time since last refresh {1}", this, Now - Kvp.Value.LastKeepAlive);
                    if (Now - Kvp.Value.LastKeepAlive > Timeout)
                    {
                        RcManager.Logger.Verbose("Removing Subscription {0}", Kvp.Value);
                        ToRemove.Add(Kvp.Value);
                    }
                }
                foreach (var Push in ToRemove)
                {
                    RemovePushDependency(Push);
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


        public static string GetPushKey(SiloAddress silo)
        {
            return silo.ToStringWithHashCode();
        }

        public static string GetPushKey(IRcManager client)
        {
            return client.GetPrimaryKey().ToString();
        }



    }

}
