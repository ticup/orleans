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

        // Double Map of "GrainId" -> "methodId(args)" -> Summary
        // TODO: This should be pushed down into the GrainActivation
        ConcurrentDictionary<GrainId, Dictionary<string, RcSummary>> SummaryMap;

        // Map of "GrainId" -> "RcSummaryWorker"
        // TODO: also move to GrainActivation
        ConcurrentDictionary<GrainId, RcSummaryWorker> WorkerMap;



        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        // FullMethodKey = InterfaceId.MethodId[Arguments]
        ConcurrentDictionary<string, RcCache> CacheMap;

        public RcManager()
        {
            SummaryMap = new ConcurrentDictionary<GrainId, Dictionary<string, RcSummary>>();
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            WorkerMap = new ConcurrentDictionary<GrainId, RcSummaryWorker>();
        }


        public int NewId()
        {
            return ++IdSequence;
        }




        #region public API
        public bool IsComputing(GrainId grainId)
        {
            RcSummaryWorker Worker;
            WorkerMap.TryGetValue(grainId, out Worker);
            return Worker != null;
        }

        public RcSummary CurrentRc()
        {
            var GrainId = RuntimeClient.Current.CurrentActivationData.GrainReference.GrainId;
            RcSummaryWorker Worker;
            WorkerMap.TryGetValue(GrainId, out Worker);
            if (Worker == null)
            {
                throw new Runtime.OrleansException("illegal state");
            }
            return Worker.Current;
        }

        public RcSummaryWorker GetRcSummaryWorker(GrainId grainId)
        {
            return WorkerMap.GetOrAdd(grainId, new RcSummaryWorker());
        }

        public ReactiveComputation<T> CreateRcWithSummary<T>(GrainId grainId, RcSource<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();
            var RcSummary = new RcRootSummary<T>(grainId, localKey, computation);
            var GrainMap = GetGrainMap(grainId);
            GrainMap.Add(localKey.ToString(), RcSummary); // TODO: refactor
            var Rc = new ReactiveComputation<T>();
            RcSummary.Subscribe(Rc);
            RcSummary.Initiate(5000, 5000);
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

                var result = await this.InitiateQuery<T>(grain, cache, request, this.CurrentRc().GetTimeout(), options, false);
                // When we received the result of this summary for the first time, we have to do a special trigger
                // such that concurrent creations of this summary can be notified of the result.
                //cache.TriggerInitialResult(result);

                // Subscribe the parent summary to this cache such that it gets notified when it's updated,
                // but only after the first result is returned, such that it does not get notified before.
                await cache.OnFirstReceived;
                //logger.Info("{0} # Got initial result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, result, ParentQuery.GetFullKey() });
                cache.TrySubscribe(this.CurrentRc());
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
                cache.TrySubscribe(this.CurrentRc());
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
        public async Task<IEnumerable<Message>> RecomputeSummaries(GrainId grainId)
        {
            Dictionary<string, RcSummary> GrainMap;
            SummaryMap.TryGetValue(grainId, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Calculate());
                await Task.WhenAll(Tasks);

                return GrainMap.Values.SelectMany(q => q.GetPushMessages());
            }
            return Enumerable.Empty<Message>();
        }

        public RcSummary<T> GetSummary<T>(GrainId grainId, string summaryKey)
        {
            return (RcSummary<T>)GetSummary(grainId, summaryKey);
        }

        public RcSummary GetSummary(GrainId grainId, string summaryKey)
        {
            Dictionary<string, RcSummary> GrainMap;
            SummaryMap.TryGetValue(grainId, out GrainMap);
            if (GrainMap != null)
            {
                RcSummary RcSummary;
                GrainMap.TryGetValue(summaryKey, out RcSummary);
                return RcSummary;
            }
            return null;
        }

        public Dictionary<string, RcSummary> GetGrainMap(GrainId grainId)
        {
            return SummaryMap.GetOrAdd(grainId, k => new Dictionary<string, RcSummary>());
        }

        public RcSummary<T> GetOrAddSummary<T>(GrainId grainId, Guid activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message, bool isRoot)
        {

            RcSummary RcSummary;
            var ActivationMethodMap = GetGrainMap(grainId);
            var MethodKey = GetMethodAndArgsKey(request);

            ActivationMethodMap.TryGetValue(MethodKey, out RcSummary);

            if (RcSummary == null)
            {
                RcSummary = new RcSummary<T>(grainId, activationKey, request, target, invoker, message.SendingAddress, timeout);
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