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
        TResult PrevResult;
        object PrevSerializedResult;

        TResult Result;
        object SerializedResult;

        IAddressable Target;
        IGrainMethodInvoker MethodInvoker;

        InvokeMethodRequest Request;

        List<PullDependency> PullsFrom = new List<PullDependency>();
        List<PushDependency> PushesTo = new List<PushDependency>();

        bool IsRoot = false;

        public Query<TResult> Query;

        // Used to construct an InternalQuery that pushes to others.
        public InternalQuery(InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, bool isRoot = false)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            IsRoot = isRoot;
            PushesTo.Add(new PushDependency(dependentSilo, dependentGrain, dependentActivation));
        }

        // Used to construct an InternalQuery that does not push to others.
        public InternalQuery(InvokeMethodRequest request, GrainReference target, bool isRoot)
        {
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

            if (Query != null)
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
                return PushesTo.Select((d) => Message.CreatePushMessage(d.TargetSilo, d.TargetGrain, d.ActivationId, Request, Result));
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
    }
}
