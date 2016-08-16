﻿using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Runtime.Reactive;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    class OutsideRcManager : RcManager, IRcManager
    {
        public static OutsideRcManager CreateRcManager()
        {
            return new OutsideRcManager();
        }

        internal Logger Logger { get; }

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        // FullMethodKey = "InterfaceId.MethodId[Arguments]"
        ConcurrentDictionary<string, RcCache> CacheMap;

        ConcurrentDictionary<string, OutsideSummaryWorker> WorkerMap;

        private Task<IRcManager> _Reference;
        public Task<IRcManager> Reference {
            get
            {
                if (_Reference == null)
                {
                    _Reference = GrainClient.GrainFactory.CreateObjectReference<IRcManager>(this);
                }
                return _Reference;  
            }
        }


        public OutsideRcManager()
        {
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            WorkerMap = new ConcurrentDictionary<string, OutsideSummaryWorker>();
            Logger = LogManager.GetLogger("OutsideRcManager");
        }

    
        public void EnqueueExecution(string summaryKey)
        {
            OutsideSummaryWorker Worker;
            WorkerMap.TryGetValue(summaryKey, out Worker);
            if (Worker == null)
            {
                throw new Exception("illegal state");
            }
            Worker.EnqueueSummary();
        }


        #region IRcManager Interface Implementation
        public Task UpdateSummaryResult(string cacheMapKey, byte[] result, Exception exception)
        {
            RcCache Cache;
            if (!CacheMap.TryGetValue(cacheMapKey, out Cache))
                return TaskDone.Done;
            Cache.OnNext(result, exception);
            return TaskDone.Done;
        }
        #endregion

        #region public API

        /// <summary>
        /// Creates a <see cref="IReactiveComputation{T}"/> from given source.
        /// This also creates a <see cref="RcRootSummary{T}"/> that internally represents the computation and its current result.
        /// The <see cref="IReactiveComputation{T}"/> is subscribed to the <see cref="RcRootSummary{T}"/> to be notified whenever its result changes.
        /// </summary>
        /// <typeparam name="T">Type of the result returned by the source</typeparam>
        /// <param name="computation">The actual computation, or source.</param>
        /// <returns></returns>
        internal ReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();

            var rc = new ReactiveComputation<T>(() =>
            {
                OutsideSummaryWorker disposed;
                WorkerMap.TryRemove(localKey.ToString(), out disposed);
            });
            var RcSummary = new RcRootSummary<T>(localKey, computation, rc, 5000);
            var scheduler = new OutsideReactiveScheduler(RcSummary);
            var worker = new OutsideSummaryWorker(RcSummary, this, scheduler);
            WorkerMap.TryAdd(localKey.ToString(), worker);
            RcSummary.EnqueueExecution();
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
        public async Task<T> ReuseOrRetrieveRcResult<T>(GrainReference grain, InvokeMethodRequest request, InvokeMethodOptions options)
        {
            T Result;
            var DependingRcSummary = this.CurrentRc();
            var activationKey = RcUtils.GetRawActivationKey(grain);
            var Key = MakeCacheMapKey(activationKey, request);
            var ncache = new RcCache<T>(this, Key);
            RcEnumeratorAsync<T> EnumAsync;
            RcCache<T> cache;
            bool existed;
            var threadSafeRetrieval = false;

            // We need to retrieve the cache and add the enumerator under the lock of the cache
            // in order to prevent interleaving with removal of the cache from the CacheMap.
            do
            {
                cache = (RcCache<T>)GetOrAddCache(activationKey, request, ncache);

                // Get an enumerator from the sub-cache dedicated to this summary
                threadSafeRetrieval = cache.GetEnumeratorAsync(DependingRcSummary, out EnumAsync);

                existed = ncache != cache;
            } while (!threadSafeRetrieval);

            //logger.Info("{0} # Initiating sub-query for caching {1}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request });
            //logger.Info("{0} # Got initial result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, result, ParentQuery.GetFullKey() });

            // First time we execute this sub-summary for the currently running summary
            if (!DependingRcSummary.HasDependencyOn(Key))
            {
                // Add the cache as a dependency to the summary
                DependingRcSummary.AddCacheDependency(Key, cache);

                // If the cache didn't exist yet, send a message to the activation
                // of the sub-summary responsible for this cache to start it
                if (!existed)
                {
                    grain.InitiateQuery<T>(request, this.CurrentRc().GetTimeout(), options);
                }

                // Wait for the first result to arrive
                Result = await EnumAsync.NextResultAsync();
                var task = HandleDependencyUpdates(Key, DependingRcSummary, EnumAsync);
            }

            // The running summary already has a dependency on this sub-summary
            else
            {
                // Flag the dependency as still valid
                DependingRcSummary.KeepDependencyAlive(Key);

                // If we already have a value in the cache for the sub-summary, just return it
                if (cache.HasValue())
                {
                    if (cache.ExceptionResult != null)
                    {
                        throw cache.ExceptionResult;
                    }
                    else
                    {
                        Result = cache.Result;
                    }
                }

                // Otherwise wait for the result to arrive using the enumerator
                else
                {
                    Result = await EnumAsync.NextResultAsync();
                }
            }

            return Result;
            //logger.Info("{0} # re-using cached result for sub-query {1} = {2} for summary {3}", new object[] { this.InterfaceId + "[" + this.GetPrimaryKey() + "]", request, cache.Result, ParentQuery.GetFullKey() });
        }

        private async Task HandleDependencyUpdates<T>(string fullMethodKey, RcSummaryBase rcSummary, RcEnumeratorAsync<T> enumAsync)
        {
            while (rcSummary.HasDependencyOn(fullMethodKey))
            {
                try
                {
                    var result = await enumAsync.NextResultAsync();
                }
                catch (ComputationStopped)
                {
                    return;
                }
                catch (TimeoutException)
                {
                    break;
                }
                catch (Exception e)
                {
                    // Re-execute the summary that depends on this value.
                    // The exception will be thrown when the summary is re-executed
                    // and this sub-summary is accessed.   
                }
                RuntimeClient.Current.EnqueueRcExecution(rcSummary.GetLocalKey());
            }
        }

        /// <summary>
        /// Gets the RcSummary that is currently being executed.
        /// </summary>
        /// <remarks>
        /// Assumes an <see cref="RcSummary"/> is currently being executed on the running task (by means of <see cref="IRuntimeClient.EnqueueRcExecution(GrainId, string)"/>) and
        /// consequently that an <see cref="RcSummaryWorker"/> has been created for this activation.
        /// </remarks>
        public RcRootSummary CurrentRc()
        {
            var Scheduler = TaskScheduler.Current;
            var RScheduler = Scheduler as OutsideReactiveScheduler;
            if (RScheduler == null)
            {
                throw new OrleansException("Illegal state");
            }
            return RScheduler.RcRootSummary;
        }
        #endregion



        #region Summary Cache API
        private RcCache GetOrAddCache(object activationKey, InvokeMethodRequest request, RcCache cache)
        {
            var Key = MakeCacheMapKey(activationKey, request);
            return CacheMap.GetOrAdd(Key, cache);
        }

        public RcCache GetCache(string cacheKey)
        {
            RcCache RcCache;
            CacheMap.TryGetValue(cacheKey, out RcCache);
            return RcCache;
        }

        public void RemoveCache(string Key)
        {
            RcCache Cache;
            CacheMap.TryRemove(Key, out Cache);
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