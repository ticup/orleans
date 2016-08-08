using Orleans.CodeGeneration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);

    /// <summary>
    /// Interface for remote calls on the reactive cache manager
    /// </summary>
    internal interface IRcManager : ISystemTarget
    {
  
    }

    internal class RcManager : SystemTarget, IRcManager
    {
        public static RcManager CreateRcManager(Silo silo)
        {
            return new RcManager(silo);
        }

        private Silo silo;

        public RcManager(Silo silo) : base(Constants.ReactiveCacheManagerId, silo.SiloAddress)
        {
            this.silo = silo;
            SummaryMap = new ConcurrentDictionary<GrainId, Dictionary<string, RcSummary>>();
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            WorkerMap = new ConcurrentDictionary<GrainId, RcSummaryWorker>();
        }

        // Keeps track of all the active summaries per GrainActivation.
        // Double Map of "GrainId" -> "LocalKey()" -> Summary
        // LocalKey() = "methodId(args)" (RcSummary) || "Guid" (RcRootSummary)
        // TODO: This could be pushed down into the ActivationData
        ConcurrentDictionary<GrainId, Dictionary<string, RcSummary>> SummaryMap;

        // Stores a SummaryWorker per GrainActivation.
        // Map of "GrainId" -> "RcSummaryWorker"
        // TODO: also move to ActivationData
        ConcurrentDictionary<GrainId, RcSummaryWorker> WorkerMap;

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        // FullMethodKey = "InterfaceId.MethodId[Arguments]"
        ConcurrentDictionary<string, RcCache> CacheMap;


        #region public API

        /// <summary>
        /// Creates a <see cref="ReactiveComputation"/> from given source.
        /// This also creates a <see cref="RcRootSummary{T}"/> that internally represents the computation and its current result.
        /// The <see cref="ReactiveComputation"/> is subscribed to the <see cref="RcRootSummary{T}"/> to be notified whenever its result changes.
        /// </summary>
        /// <typeparam name="T">Type of the result returned by the source</typeparam>
        /// <param name="grainId">The id of the activation this computation runs on</param>
        /// <param name="computation">The actual computation, or source.</param>
        /// <returns></returns>
        public ReactiveComputation<T> CreateReactiveComputation<T>(GrainId grainId, Func<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();
            var RcSummary = new RcRootSummary<T>(grainId, localKey, computation);
            var GrainMap = GetGrainMap(grainId);
            GrainMap.Add(localKey.ToString(), RcSummary); // TODO: refactor
            var Rc = new ReactiveComputation<T>();
            RcSummary.Subscribe(Rc);
            RcSummary.Start(5000, 5000);
            return Rc;
        }


        /// <summary>
        /// Intercepts the call to a subquery.
        /// This will either get the existing value in the cache if it exists or create the cache and ask the grain this computation belongs to to start the computation.
        /// </summary>
        /// <remarks>
        /// This is assumed to be running within a task that is executing a reactive computation, i.e. after testing <see cref="IRuntimeClient.InReactiveComputation"/>
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="grain"></param>
        /// <param name="request"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task<T> ReuseOrRetrieveRcResult<T>(GrainId dependentGrain, GrainReference grain, InvokeMethodRequest request, InvokeMethodOptions options)
        {
            var activationKey = grain.GetPrimaryKey();
            var cache = new RcCache<T>();
            var didNotExist = TryAddCache(activationKey, request, cache);
            var DependingRcSummary = this.CurrentRc();


            // First time we initiate this summary, so we have to actually invoke it and set it up in the target grain
            if (didNotExist)
            {
                //logger.Info("{0} # Initiating sub-query for caching {1}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request });
                grain.InitiateQuery<T>(request, this.CurrentRc().GetTimeout(), options);
                //logger.Info("{0} # Got initial result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, result, ParentQuery.GetFullKey() });
            }

            // Already have a cache for this summary in the runtime
            else
            {
                grain.SubscribeQuery<T>(request, this.CurrentRc().GetTimeout(), options);
                // TODO!!: this is still incorrect.
                // Get the existing cache
                cache = GetCache<T>(activationKey, request);
                if (cache.Result != null)
                {
                    return cache.Result;
                } else
                {
                    var enumAsync = cache.GetEnumeratorAsync(dependentGrain, DependingRcSummary);
                    return await enumAsync.OnUpdateAsync();
                }
                //logger.Info("{0} # re-using cached result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, cache.Result, ParentQuery.GetFullKey() });
            }
            
            // Concurrently using this cached method, it might not be resolved yet
            var EnumAsync = cache.GetEnumeratorAsync(dependentGrain, DependingRcSummary);
            var result = await EnumAsync.OnUpdateAsync();
            var ctx = RuntimeContext.CurrentActivationContext;
            var task = HandleDependencyUpdates<T>(DependingRcSummary, EnumAsync, ctx);
            return result;
        }

        private async Task HandleDependencyUpdates<T>(RcSummary rcSummary, RcEnumeratorAsync<T> enumAsync, ISchedulingContext ctx)
        {
            // TODO: while (computationAlive)
            while (true)
            {
                var result = await enumAsync.OnUpdateAsync();
                var task = RuntimeClient.Current.ExecAsync(()=> rcSummary.Calculate(), ctx, "Update Dependencies");
            }
        }

        /// <summary>
        /// Gets the RcSummary that is currently being executed.
        /// </summary>
        /// <remarks>
        /// Assumes an <see cref="RcSummary"/> is currently being executed on the running task (by means of <see cref="IRuntimeClient.EnqueueRcExecution(GrainId, string)"/>) and
        /// consequently that an <see cref="RcSummaryWorker"/> has been created for this activation.
        /// </remarks>
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
        

        /// <summary>
        /// Gets the <see cref="RcSummaryWorker"/> for a given grain activation,
        /// if it doesn't exists yet it will be created.
        /// </summary>
        /// <param name="grainId">The id of the grain activation of the requested <see cref="RcSummaryWorker"/></param>
        /// <returns></returns>
        public RcSummaryWorker GetRcSummaryWorker(GrainId grainId)
        {
            return WorkerMap.GetOrAdd(grainId, new RcSummaryWorker());
        }
        #endregion








        #region Summary Cache API
        /// <summary>
        /// Updates the <see cref="RcCache"/> with given new result.
        /// </summary>
        /// <param name="activationKey">Key of the activation for the request</param>
        /// <param name="request">The request that together with the key uniquely identifies the invocation on a particular activation</param>
        /// <param name="result">Dependency updates happen before this <see cref="Task"/> returns.</param>
        /// <returns></returns>
        //public Task UpdateCache(Guid activationKey, InvokeMethodRequest request, object result)
        //{
        //    RcCache Cache = GetCache(activationKey, request);
        //    return Cache.TriggerUpdate(result);
        //}

        public void NotifyDependentsOfCache(GrainId grainId, Guid activationKey, InvokeMethodRequest request, object result)
        {
            var cache = GetCache(activationKey, request);
            cache.OnNext(grainId, result);
        }

        /// <summary>
        /// If this cache is dirty (its result has a new value), it gets all the <see cref="Message"/> to notify the <see cref="RcSummaryWorker"/> that depend on this cache.
        /// </summary>
        /// <param name="activationKey">Key of the activation for the request</param>
        /// <param name="request">The request that together with the key uniquely identifies the invocation on a particular activation</param>
        /// <returns></returns>
        //public IEnumerable<Message> GetPushMessagesForCache(Guid activationKey, InvokeMethodRequest request)
        //{
        //    RcCache Cache = GetCache(activationKey, request);
        //    return Cache.GetPushMessages();
        //}

        /// <summary>
        /// Gets the <see cref="RcCache"/>
        /// </summary>
        /// <remarks>
        /// Only use this method if you know the type of this cache.
        /// </remarks>
        /// <typeparam name="T">Type used to cast the result to</typeparam>
        /// <param name="activationKey">Key of the activation for the request</param>
        /// <param name="request">The request that together with the key uniquely identifies the invocation on a particular activation</param>
        /// <returns></returns>
        public RcCache<T> GetCache<T>(Guid activationKey, InvokeMethodRequest request)
        {
            return (RcCache<T>)GetCache(activationKey, request);
        }

        /// <summary>
        /// Gets the <see cref="RcCache"/>
        /// </summary>
        /// <param name="activationKey">Key of the activation for the request</param>
        /// <param name="request">The request that together with the key uniquely identifies the invocation on a particular activation</param>
        /// <returns></returns>
        public RcCache GetCache(Guid activationKey, InvokeMethodRequest request)
        {
            RcCache Cache;
            var Key = GetFullMethodKey(activationKey, request);
            CacheMap.TryGetValue(Key, out Cache);
            return Cache;
        }

        /// <summary>
        /// Tries to concurrently install given cache.
        /// If the install failed it means a cache is already in place and it should be retrieved with <see cref="GetCache(Guid, InvokeMethodRequest)"/>
        /// </summary>
        /// <returns>True if it succeed, false otherwise.</returns>
        private bool TryAddCache(Guid activationKey, InvokeMethodRequest request, RcCache cache)
        {
            var Key = GetFullMethodKey(activationKey, request);
            return CacheMap.TryAdd(Key, cache);
        }

        #endregion


        #region Summary API
        /// <summary>
        /// Reschedules calculation of the reactive computations (<see cref="RcSummary"/>) that belong to the given grain activation.
        /// </summary>
        /// <param name="grainId">Id of the grain activation</param>
        /// <returns>The <see cref="Message"/> instances that represent notification of the invalided caches to the depedent grain activations</returns>
        public async Task RecomputeSummaries(GrainId grainId)
        {
            Dictionary<string, RcSummary> GrainMap;
            SummaryMap.TryGetValue(grainId, out GrainMap);
            if (GrainMap != null)
            {
                var Tasks = GrainMap.Values.Select(q => q.Calculate());
                await Task.WhenAll(Tasks);
            }
        }


        /// <summary>
        /// Gets the <see cref="RcSummary"/> that is identified by given grain activation and the local key.
        /// </summary>
        /// <param name="grainId">Id of the grain activation.</param>
        /// <param name="localKey"><see cref="RcSummary.GetLocalKey()"/></param>
        /// <returns></returns>
        public RcSummary GetSummary(GrainId grainId, string localKey)
        {
            Dictionary<string, RcSummary> GrainMap;
            SummaryMap.TryGetValue(grainId, out GrainMap);
            if (GrainMap != null)
            {
                RcSummary RcSummary;
                GrainMap.TryGetValue(localKey, out RcSummary);
                return RcSummary;
            }
            return null;
        }


        /// <summary>
        /// Concurrently gets or creates a <see cref="RcSummary"/> for given activation and request.
        /// </summary>
        public async Task CreateAndStartSummary<T>(GrainId grainId, Guid activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message, bool isRoot)
        {
            RcSummary RcSummary;
            var ActivationMethodMap = GetGrainMap(grainId);
            var MethodKey = GetMethodAndArgsKey(request);

            ActivationMethodMap.TryGetValue(MethodKey, out RcSummary);

            if (RcSummary == null)
            {
                RcSummary = new RcSummary<T>(grainId, activationKey, request, target, invoker, message.SendingAddress, timeout);
                ActivationMethodMap.Add(MethodKey, RcSummary);
                await RcSummary.Calculate();
            }
            else
            {
                RcSummary.GetOrAddPushDependency(message.SendingAddress, timeout);
            }
        }
        #endregion


        /// <summary>
        /// Gets the Map of <see cref="RcSummary"/> that belong to given activation.
        /// </summary>
        /// <param name="grainId">The id of the grain activation</param>
        private Dictionary<string, RcSummary> GetGrainMap(GrainId grainId)
        {
            return SummaryMap.GetOrAdd(grainId, k => new Dictionary<string, RcSummary>());
        }


        #region Identifier Retrievers
        public static string GetFullMethodKey(Guid activationKey, InvokeMethodRequest request)
        {
            return GetFullActivationKey(request.InterfaceId, activationKey) + "." + GetMethodAndArgsKey(request);
        }

        public static string GetFullActivationKey(int interfaceId, Guid activationKey)
        {
            return interfaceId + "[" + activationKey + "]";
        }

        public static string GetMethodAndArgsKey(InvokeMethodRequest request)
        {
            return request.MethodId + "(" + Utils.EnumerableToString(request.Arguments) + ")";
        }
        #endregion

    }
}