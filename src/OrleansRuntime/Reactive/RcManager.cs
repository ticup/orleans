﻿using Orleans.CodeGeneration;
using Orleans.Reactive;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation<T> ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);

    /// <summary>
    /// Interface for remote calls on the reactive cache manager
    /// </summary>
    internal interface IRcManager : ISystemTarget
    {
        /// <summary>
        /// Update the cached result of a summary.
        /// </summary>
        /// <returns>true if cache is actively used, or false if cache no longer exists</returns>
        Task<bool> UpdateSummaryResult(string cacheMapKey, byte[] result, Exception exception);
    }



    internal class RcManager : SystemTarget, IRcManager
    {
        public static RcManager CreateRcManager(Silo silo)
        {
            return new RcManager(silo);
        }

        private Silo silo;
        internal Logger Logger { get; }

        public RcManager(Silo silo) : base(Constants.ReactiveCacheManagerId, silo.SiloAddress)
        {
            this.silo = silo;
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            Logger = LogManager.GetLogger("RcManager");
        }

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        // FullMethodKey = "InterfaceId.MethodId[Arguments]"
        ConcurrentDictionary<string, RcCache> CacheMap;


        #region public API

        /// <summary>
        /// Creates a <see cref="IReactiveComputation{T}"/> from given source.
        /// This also creates a <see cref="RcRootSummary{T}"/> that internally represents the computation and its current result.
        /// The <see cref="IReactiveComputation{T}"/> is subscribed to the <see cref="RcRootSummary{T}"/> to be notified whenever its result changes.
        /// </summary>
        /// <typeparam name="T">Type of the result returned by the source</typeparam>
        /// <param name="computation">The actual computation, or source.</param>
        /// <returns></returns>
        internal IReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();
            var SummaryMap = GetCurrentSummarymap();
            var rc = new ReactiveComputation<T>(() => {
                RcSummary disposed;
                SummaryMap.TryRemove(localKey.ToString(), out disposed);
            });
            var RcSummary = new RcRootSummary<T>(localKey, computation, rc);
            var success = SummaryMap.TryAdd(localKey.ToString(), RcSummary);
            if (!success)
            {
                throw new OrleansException("Illegal State");
            }
            RcSummary.Start(5000, 5000);
            return rc;
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
            T Result;
            var activationKey = InsideRuntimeClient.GetRawActivationKey(grain);
            var Key = MakeCacheMapKey(activationKey, request);
            var cache = new RcCache<T>();
            var exists = !TryAddCache(activationKey, request, cache);
            var DependingRcSummary = this.CurrentRc();

            // Already have a cache for this summary in the runtime, get it.
            if (exists)
            {
                cache = GetCache<T>(activationKey, request);
            }
            //logger.Info("{0} # Initiating sub-query for caching {1}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request });
            //logger.Info("{0} # Got initial result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, result, ParentQuery.GetFullKey() });
            
        
            // Get the Enumerator for the sub-summary from the currently running summary.
            var EnumAsync = (RcEnumeratorAsync<T>)DependingRcSummary.GetDependencyEnum(Key);

            // First time we execute this sub-summary for the currently running summary.
            if (EnumAsync == null)
            {
                EnumAsync = cache.GetEnumeratorAsync(DependingRcSummary);
                DependingRcSummary.AddDependencyEnum(Key, EnumAsync);
                grain.InitiateQuery<T>(request, this.CurrentRc().GetTimeout(), options);
                var ctx = RuntimeContext.CurrentActivationContext;
                Result = await EnumAsync.NextResultAsync();
                var task = HandleDependencyUpdates(Key, DependingRcSummary, EnumAsync, ctx);
            }

            // We already have a value in the cache, immediately return
            else if (cache.HasValue())
            {
                if (cache.ExceptionResult != null)
                {
                    throw cache.ExceptionResult;
                } else
                {
                    Result = cache.Result;
                }
            }

            // Otherwise, wait for the result to arrive using the Enumerator
            else
            {
                Result = await EnumAsync.NextResultAsync();
            }

            return Result;
            //logger.Info("{0} # re-using cached result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, cache.Result, ParentQuery.GetFullKey() });
        }

        private async Task HandleDependencyUpdates<T>(string fullMethodKey, RcSummary rcSummary, RcEnumeratorAsync<T> enumAsync, ISchedulingContext ctx)
        {
            while (rcSummary.HasDependencyOn(fullMethodKey))
            {
                try
                {
                    var result = await enumAsync.NextResultAsync();
                    
                } catch (Exception e)
                {
                    // Do nothing, the exception will be thrown when the summary is re-executed
                    // and this sub-summary is accessed.
                }
                var task = RuntimeClient.Current.ExecAsync(() =>
                {
                    return rcSummary.EnqueueExecution();
                }, ctx, "Update Dependencies");
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
            var Worker = GetCurrentWorker();
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
        public RcSummaryWorker GetCurrentWorker()
        {
            return ((ActivationData)RuntimeClient.Current.CurrentActivationData).RcSummaryWorker;
        }
        #endregion








        #region Summary Cache API
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
        public RcCache<T> GetCache<T>(object activationKey, InvokeMethodRequest request)
        {
            return (RcCache<T>)GetCache(activationKey, request);
        }

        /// <summary>
        /// Gets the <see cref="RcCache"/>
        /// </summary>
        /// <param name="activationKey">Key of the activation for the request</param>
        /// <param name="request">The request that together with the key uniquely identifies the invocation on a particular activation</param>
        /// <returns></returns>
        public RcCache GetCache(object activationKey, InvokeMethodRequest request)
        {
            RcCache Cache;
            var Key = MakeCacheMapKey(activationKey, request);
            CacheMap.TryGetValue(Key, out Cache);
            return Cache;
        }

        /// <summary>
        /// Tries to concurrently install given cache.
        /// If the install failed it means a cache is already in place and it should be retrieved with <see cref="GetCache(Guid, InvokeMethodRequest)"/>
        /// </summary>
        /// <returns>True if it succeed, false otherwise.</returns>
        private bool TryAddCache(object activationKey, InvokeMethodRequest request, RcCache cache)
        {
            var Key = MakeCacheMapKey(activationKey, request);
            return CacheMap.TryAdd(Key, cache);
        }


        public Task<bool> UpdateSummaryResult(string cacheMapKey, byte[] result, Exception exception)
        {
            RcCache Cache;
            if (! CacheMap.TryGetValue(cacheMapKey, out Cache))
                return Task.FromResult(false);
            Cache.OnNext(result, exception);
            return Task.FromResult(true);
        }

    #endregion


    #region Summary API
        public ConcurrentDictionary<string, RcSummary> GetCurrentSummarymap()
        {
            return ((ActivationData)RuntimeClient.Current.CurrentActivationData).RcSummaryMap;
        }

    /// <summary>
    /// Reschedules calculation of the reactive computations of the current activation.
    /// </summary>
    public async Task RecomputeSummaries()
        {
            var Tasks = GetCurrentSummarymap().Values.Select(q => q.EnqueueExecution());
            await Task.WhenAll(Tasks);
        }


        /// <summary>
        /// Gets the <see cref="RcSummary"/> identified by the local key and current activation
        /// </summary>
        /// <param name="localKey"><see cref="RcSummary.GetLocalKey()"/></param>
        /// <returns></returns>
        public RcSummary GetSummary(string localKey)
        {
            RcSummary RcSummary;
            var SummaryMap = GetCurrentSummarymap();
            SummaryMap.TryGetValue(localKey, out RcSummary);
            return RcSummary;
        }


        /// <summary>
        /// Concurrently gets or creates a <see cref="RcSummary"/> for given activation and request.
        /// </summary>
        public async Task CreateAndStartSummary<T>(object activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message, bool isRoot)
        {
            RcSummary RcSummary;
            var SummaryMap = GetCurrentSummarymap();
            var MethodKey = GetMethodAndArgsKey(request);

            RcSummary = new RcSummary<T>(activationKey, request, target, invoker, message.SendingAddress, timeout);
            var existed = !SummaryMap.TryAdd(MethodKey, RcSummary);

            if (existed)
            {
                SummaryMap.TryGetValue(MethodKey, out RcSummary);
                await RcSummary.GetOrAddPushDependency(message.SendingAddress.Silo, timeout);
            } else
            {
                await RcSummary.EnqueueExecution();
            }
        }
        #endregion

        #region Identifier Retrievers
        public static string MakeCacheMapKey(object activationKey, InvokeMethodRequest request)
        {
            return GetFullActivationKey(request.InterfaceId, activationKey) + "." + GetMethodAndArgsKey(request);
        }

        public static string GetFullActivationKey(int interfaceId, object activationKey)
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