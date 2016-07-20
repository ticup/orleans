using Orleans.CodeGeneration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);

    internal interface IRcManager
    {
    }

        internal class RcManager : IRcManager
    {
        private int IdSequence = 0;

        // Double Map of "InterfaceId[Activation Key]" -> "methodId(args)" -> Summary
        // TODO: This should be pushed down into the GrainActivation
        ConcurrentDictionary<string, Dictionary<string, RcSummary>> SummaryMap;

        // Map of "InterfaceId[Activation Key]" -> "RcSummary" to keep track of the current running query in an activation.
        ConcurrentDictionary<string, RcSummary> ParentComputationMap;


        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        ConcurrentDictionary<string, RcCache> CacheMap;

        public RcSummary CurrentRc;

        public RcManager()
        {
            SummaryMap = new ConcurrentDictionary<string, Dictionary<string, RcSummary>>();
            CacheMap = new ConcurrentDictionary<string, RcCache>();

            CurrentRc = null;
        }


        public int NewId()
        {
            return ++IdSequence;
        }


        public bool IsExecutingRc()
        {
            return CurrentRc != null;
        }




        #region public API
        public bool IsComputing()
        {
            return this.CurrentRc != null;
        }

        public async Task<object> InvokeSubComputationFor(RcSummary summary)
        {
            var ParentRc = CurrentRc;
            CurrentRc = summary;
            var ResultT = summary.Execute();
            var Result = await ResultT;
            CurrentRc = ParentRc;
            return Result;
        }

        public async Task<ReactiveComputation<T>> CreateRcWithSummary<T>(RcSource<Task<T>> computation)
        {
            var RcSummary = new RcRootSummary<T>(Guid.NewGuid(), computation);
            var Result = await RcSummary.Initiate(5000, 5000);
            var Rc = new ReactiveComputation<T>((T)Result);
            RcSummary.Subscribe(Rc);
            return Rc;
        }


        /// <summary>
        /// Is assumed to be run within a parent computation
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="grain"></param>
        /// <param name="request"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task<T> ReuseOrRetrieveRcResult<T>(GrainReference grain, InvokeMethodRequest request, InvokeMethodOptions options)
        {
            var activationKey = grain.GetPrimaryKey();
            var cache = new RcCache<T>();
            var didNotExist = TryAddCache(activationKey, request, cache);


            // First time we initiate this summary, so we have to actually invoke it and set it up in the target grain
            if (didNotExist)
            {
                //logger.Info("{0} # Initiating sub-query for caching {1}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request });

                var result = await this.InitiateQuery<T>(grain, cache, request, this.CurrentRc.GetTimeout(), options, false);
                // When we received the result of this summary for the first time, we have to do a special trigger
                // such that concurrent creations of this summary can be notified of the result.
                //cache.TriggerInitialResult(result);

                // Subscribe the parent summary to this cache such that it gets notified when it's updated,
                // but only after the first result is returned, such that it does not get notified before.
                await cache.OnFirstReceived;
                //logger.Info("{0} # Got initial result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, result, ParentQuery.GetFullKey() });
                cache.TrySubscribe(this.CurrentRc);
                return result;
            }

            // Already have a cache for this summary in the runtime
            else
            {
                // Get the existing cache
                cache = GetCache<T>(activationKey, request);
                // Concurrently using this cached method, it might not be resolved yet
                await cache.OnFirstReceived;
                //logger.Info("{0} # re-using cached result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, cache.Result, ParentQuery.GetFullKey() });
                cache.TrySubscribe(this.CurrentRc);
                return cache.Result;
            }
        }


        public async Task<T> InitiateQuery<T>(GrainReference grain, RcCache<T> cache, InvokeMethodRequest request, int timeout, InvokeMethodOptions options, bool root)
        {
            var Result = await grain.InitiateQuery<T>(request, timeout, options);
            cache.TriggerInitialResult(Result);
            return Result;
        }

        #endregion








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
        public async Task<IEnumerable<Message>> RecomputeSummaries(int interfaceId, Guid activationKey)
        {
            Dictionary<string, RcSummary> GrainMap;
            var Key = GetFullActivationKey(interfaceId, activationKey);
            SummaryMap.TryGetValue(Key, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Recalculate());
                await Task.WhenAll(Tasks);

                return GrainMap.Values.SelectMany(q => q.GetPushMessages());
            }
            return Enumerable.Empty<Message>();
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

        public RcSummary<T> GetOrAddSummary<T>(Guid activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message, bool isRoot)
        {

            RcSummary RcSummary;
            var Key = GetFullActivationKey(request.InterfaceId, activationKey);
            var ActivationMethodMap = SummaryMap.GetOrAdd(Key, k => new Dictionary<string, RcSummary>());
            var MethodKey = GetMethodAndArgsKey(request);

            ActivationMethodMap.TryGetValue(MethodKey, out RcSummary);

            if (RcSummary == null)
            {
                RcSummary = new RcSummary<T>(activationKey, request, target, invoker, message.SendingAddress, timeout);
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