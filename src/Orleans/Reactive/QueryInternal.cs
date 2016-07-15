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
    interface QueryInternal : IQueryCacheObserver
    {
        Task Recalculate();
        IEnumerable<Message> GetPushMessages();

        string GetMethodAndArgsKey();
        int GetInterfaceId();

        string GetFullKey();

        //int GetQueryId();

        int GetTimeout();

        PushDependency GetOrAddPushDependency(SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, ActivationAddress activationAddress, int timeout);

    }

    class QueryInternal<TResult> : QueryInternal
    {
        private TResult PrevResult;
        private byte[] PrevSerializedResult;

        public TResult Result { get; private set; }
        private byte[] SerializedResult;

        private IAddressable Target;
        private IGrainMethodInvoker MethodInvoker;
        private Guid ActivationKey;

        private InvokeMethodRequest Request;

        private Dictionary<string, PushDependency> PushesTo = new Dictionary<string, PushDependency>();

        private bool IsRoot = false;

        private int Timeout;

        // Used to construct an InternalQuery that pushes to others.
        public QueryInternal(Guid activationKey, InvokeMethodRequest request, IAddressable target, IGrainMethodInvoker invoker, SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, ActivationAddress dependentAddress, int timeout, bool isRoot)
        {
            Request = request;
            Target = target;
            MethodInvoker = invoker;
            IsRoot = isRoot;
            ActivationKey = activationKey;
            Timeout = timeout;
            var key = GetDependentKey(dependentSilo, dependentGrain, dependentActivation);
            PushesTo.Add(key, new PushDependency(dependentSilo, dependentGrain, dependentActivation, dependentAddress, timeout));
        }
        

        public void SetInitialResult(TResult result)
        {
            PrevResult = result;
            BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
            Serialization.SerializationManager.Serialize(result, stream);
            PrevSerializedResult = stream.ToByteArray();


            Result = PrevResult;
            SerializedResult = PrevSerializedResult;
        }

        public void SetResult(TResult result)
        {
            if (Result == null)
            {
                SetInitialResult(result);
            }
            else
            {
                PrevResult = Result;
                PrevSerializedResult = SerializedResult;

                Result = result;
                BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
                Serialization.SerializationManager.Serialize(result, stream);
                SerializedResult = stream.ToByteArray();
            }
        }

        // This is called whenever one of the queries we depend on has its value changed (ignore the result).
        public async Task OnNext(object result)
        {
            await Recalculate();
        }
        

        public IEnumerable<Message> GetPushMessages()
        {
            if (!SerializationManager.CompareBytes(PrevSerializedResult, SerializedResult))
            {
                return PushesTo.Values.Select((d) => Message.CreatePushMessage(ActivationKey, d.ActivationAddress, Request, Result));
            }
            else
            {
                return new List<Message>();
            }
        }

        public string GetKey()
        {
            return GetFullKey();
        }

        // To be used for inter-train identification
        public string GetFullKey()
        {
            return GetInterfaceId() + "." + GetMethodAndArgsKey();
        }

        // Only to be used for intra-grain identification of queries
        //public string GetKey()
        //{
        //    return IdNumber.ToString();
        //}






        public string GetMethodAndArgsKey()
        {
            return QueryManager.GetMethodAndArgsKey(Request);
        }

        public int GetInterfaceId()
        {
            return Request.InterfaceId;
        }

        //public int GetQueryId()
        //{
        //    return IdNumber;
        //}

        public int GetTimeout()
        {
            return Timeout;
        }

        //public InternalQuery(IAddressable target, IGrainMethodInvoker invoker, TResult result)
        //{
        //    Target = target;
        //    MethodInvoker = invoker;
        //    Result = result;
        //    SerializedResult = Serialization.SerializationManager.DeepCopy(Result);
        //}

        public async Task Recalculate()
        {
            var oldResult = Result;
            var oldSerializedResult = SerializedResult;
            var ParentQuery = RuntimeClient.Current.QueryManager.CurrentQuery;
            RuntimeClient.Current.QueryManager.CurrentQuery = this;
            var resWrap = (await MethodInvoker.Invoke(Target, Request));
            RuntimeClient.Current.QueryManager.CurrentQuery = ParentQuery;
            TResult res;
            if (IsRoot)
            {
                res = ((Query<TResult>)resWrap).Result;
            }
            else
            {
                res = (TResult)resWrap;
            }
            SetResult(res);
        }

        public PushDependency GetOrAddPushDependency(SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation, ActivationAddress dependentAddress, int timeout)
        {
            var Key = GetDependentKey(dependentSilo, dependentGrain, dependentActivation);
            PushDependency Push;
            PushesTo.TryGetValue(Key, out Push);
            if (Push == null)
            {
                Push = new PushDependency(dependentSilo, dependentGrain, dependentActivation, dependentAddress, timeout);
                PushesTo.Add(Key, Push);
            }
            return Push;
        }


        private static string GetDependentKey(SiloAddress dependentSilo, GrainId dependentGrain, ActivationId dependentActivation)
        {
            return dependentSilo.ToString() + dependentGrain.ToString() + dependentActivation.ToString();
        }


    }

}
