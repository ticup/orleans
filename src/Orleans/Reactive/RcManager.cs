using Orleans.CodeGeneration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);
    class RcManager
    {
        private int IdSequence = 0;

        // Double Map of "InterfaceId[Activation Key]" -> "methodId(args)" -> Summary
        // TODO: This should be pushed down into the GrainActivation
        ConcurrentDictionary<string, Dictionary<string, RcSummary>> SummaryMap;

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        ConcurrentDictionary<string, RcCache> CacheMap;

        public RcSummary CurrentRc { get; set; }

        public RcManager()
        {
            SummaryMap = new ConcurrentDictionary<string, Dictionary<string, RcSummary>>();
            CacheMap   = new ConcurrentDictionary<string, RcCache>();

            CurrentRc = null;
        }


        public int NewId()
        {
            return ++IdSequence;
        }


        public bool IsExecutingRc() {
            return CurrentRc != null;
        }



        #region Thread-safe Cache API
        public IEnumerable<Message> GetPushMessagesForCache(Guid activationKey, InvokeMethodRequest request)
        {
            RcCache Cache = GetCache(activationKey, request);
            return Cache.GetPushMessages();
        }

        // Updates the cache of a summary, identified by the activation key and the InvokeMethodRequest, with given result.
        // Dependencies are notified and updated because they observe this cache.
        // Dependency updates happen before this Task returns.
        public Task UpdateCache(Guid activationKey, InvokeMethodRequest request, object result)
        {
            RcCache Cache = GetCache(activationKey, request);
            return Cache.TriggerUpdate(result);
        }

        public RcCache<T> GetCache<T>(Guid activationKey, InvokeMethodRequest request)
        {
            return (RcCache<T>)GetCache(activationKey, request);
        }

        public RcCache GetCache(Guid activationKey, InvokeMethodRequest request)
        {
            RcCache Cache;
            var Key = GetFullMethodKey(activationKey, request);
            CacheMap.TryGetValue(Key, out Cache);
            return Cache;
        }

        public bool TryAddCache(Guid activationKey, InvokeMethodRequest request, RcCache cache)
        {
            try
            {
                var Key = GetFullMethodKey(activationKey, request);
                return CacheMap.TryAdd(Key, cache);
                // DEBUG purposes
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        #endregion


        #region Summary API
        public async Task<IEnumerable<IEnumerable<Message>>> RecomputeSummaries(int interfaceId, Guid activationKey)
        {
            Dictionary<string, RcSummary> GrainMap;
            var Key = GetFullActivationKey(interfaceId, activationKey);
            SummaryMap.TryGetValue(Key, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Recalculate());
                await Task.WhenAll(Tasks);

                return GrainMap.Values.Select(q => q.GetPushMessages());
            }
            return Enumerable.Empty<IEnumerable<Message>>();
        }

        public RcSummary<T> GetSummary<T>(Guid activationKey, InvokeMethodRequest request)
        {
            return (RcSummary<T>)GetSummary(activationKey, request);
        }

        public RcSummary GetSummary(Guid activationKey, InvokeMethodRequest request)
        {
            Dictionary<string, RcSummary> GrainMap;
            var Key = GetFullActivationKey(request.InterfaceId, activationKey);
            SummaryMap.TryGetValue(Key, out GrainMap);
            if (GrainMap != null)
            {
                RcSummary RcSummary;
                GrainMap.TryGetValue(GetMethodAndArgsKey(request), out RcSummary);
                return RcSummary;
            }
            return null;
        }

        public RcSummary<T> GetOrAddSummary<T>(Guid activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout,  Message message, bool isRoot)
        {

            RcSummary RcSummary;
            var Key = GetFullActivationKey(request.InterfaceId, activationKey);
            var ActivationMethodMap = SummaryMap.GetOrAdd(Key, k => new Dictionary<string, RcSummary>());
            var MethodKey = GetMethodAndArgsKey(request);

            ActivationMethodMap.TryGetValue(MethodKey, out RcSummary);

            if (RcSummary == null)
            {
                RcSummary = new RcSummary<T>(activationKey, request, target, invoker, message.SendingAddress, timeout, isRoot);
                ActivationMethodMap.Add(MethodKey, RcSummary);
            }
            else
            {
                RcSummary.GetOrAddPushDependency(message.SendingAddress, timeout);
            }
            return (RcSummary<T>)RcSummary;
        }
        #endregion


        #region Identifier Retrievers

        public static string GetFullActivationKey(int interfaceId, Guid activationKey)
        {
            return interfaceId + "[" + activationKey + "]";
        }

        public static string GetFullMethodKey(Guid activationKey, InvokeMethodRequest request)
        {
            return GetFullActivationKey(request.InterfaceId, activationKey) + "." + GetMethodAndArgsKey(request);
        }

        public static string GetMethodAndArgsKey(InvokeMethodRequest request)
        {
            return request.MethodId + "(" + Utils.EnumerableToString(request.Arguments) + ")";
        }
        
        #endregion

    }
}