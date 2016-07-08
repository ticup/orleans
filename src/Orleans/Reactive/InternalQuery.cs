using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    interface InternalQuery
    {
        Task<IEnumerable<Message>> Recalculate();
        string GetMethodAndArgsKey();
        int GetInterfaceId();
    }

    class InternalQuery<TResult> : InternalQuery
    {
        private static int IdSequence = 0;

        public int IdNumber { get; private set; }
        private TResult PrevResult;
        private object PrevSerializedResult;

        public TResult Result { get; private set; }
        private object SerializedResult;

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;

        private InvokeMethodRequest Request;

        private List<PullDependency> PullsFrom = new List<PullDependency>();
        private Dictionary<string, PushDependency> PushesTo = new Dictionary<string, PushDependency>();

        private bool IsRoot = false;

        private List<Query<TResult>> Queries = new List<Query<TResult>>();


        // Used to construct an InternalQuery that pushes to others.
        public InternalQuery(InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, int dependingIdNumber, int timeout, bool isRoot = false)
        {
            IdNumber = ++IdSequence;
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            IsRoot = isRoot;
            var key = GetDependentKey(dependentSilo, dependentGrain, dependentActivation);
            PushesTo.Add(key, new PushDependency(dependingIdNumber, dependentSilo, dependentGrain, dependentActivation, timeout));
        }
        // Used to construct an InternalQuery that does not push to others.
        public InternalQuery(InvokeMethodRequest request, GrainReference target, bool isRoot)
        {
            IdNumber = ++IdSequence;
            Request = request;
            Target = target;
            IsRoot = isRoot;
        }

        public void SetInitialResult(TResult result)
        {
            PrevResult = result;
            PrevSerializedResult = Serialization.SerializationManager.DeepCopy(result);

            Result = PrevResult;
            SerializedResult = PrevSerializedResult;
        }

        public void SetResult(TResult result)
        {
            PrevResult = Result;
            PrevSerializedResult = SerializedResult;

            Result = result;
            SerializedResult = Serialization.SerializationManager.DeepCopy(Result);
        }

        public IEnumerable<Message> TriggerUpdate(TResult result)
        {
            SetResult(result);

            foreach (var Query in Queries)
            {
                Query.TriggerUpdate(result);
            }
            
            return GetPushMessages();
        }

        public IEnumerable<Message> GetPushMessages()
        {
            IEnumerable<Message> Msgs;
            // TODO: proper bytecheck check
            if (PrevSerializedResult !=
                SerializedResult)
            {
                return PushesTo.Values.Select((d) => Message.CreatePushMessage(d.TargetSilo, d.TargetGrain, d.ActivationId, Request, Result));
            }
            else
            {
                return new List<Message>();
            }
        }

        public string GetMethodAndArgsKey()
        {
            return QueryManager.GetMethodAndArgsKey(Request.MethodId, Request.Arguments);
        }

        public int GetInterfaceId()
        {
            return Request.InterfaceId;
        }

        //public InternalQuery(IAddressable target, IGrainMethodInvoker invoker, TResult result)
        //{
        //    Target = target;
        //    MethodInvoker = invoker;
        //    Result = result;
        //    SerializedResult = Serialization.SerializationManager.DeepCopy(Result);
        //}

        public async Task<IEnumerable<Message>> Recalculate()
        {
            var oldResult = Result;
            var oldSerializedResult = SerializedResult;
            var resWrap = (await MethodInvoker.Invoke(Target, Request));
            TResult res;
            if (IsRoot)
            {
                res = ((Query<TResult>)resWrap).Result;
            } else
            {
                res = (TResult)resWrap;
            }
            return TriggerUpdate(res);
        }

        public void AddQuery(Query<TResult> Query)
        {
            Queries.Add(Query);
        }

        public void AddPushDependency(SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, int dependingIdNumber, int timeout)
        {
            var Key = GetDependentKey(dependentSilo, dependentGrain, dependentActivation);
            PushDependency push;
            PushesTo.TryGetValue(Key, out push);
            if (push != null)
            {
                push.AddQueryDependency(dependingIdNumber, timeout);
            } else
            {
                PushesTo.Add(Key, new PushDependency(dependingIdNumber, dependentSilo, dependentGrain, dependentActivation, timeout));
            }
        }




        private static string GetDependentKey(SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation)
        {
            return dependentSilo.ToString() + dependentGrain.ToString() + dependentActivation.ToString();
        }


    }

}
