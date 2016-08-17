using Orleans.CodeGeneration;
using Orleans.Reactive;
using Orleans.Runtime.Reactive;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{

    abstract class RcManagerBase : RcManager, IRcManager
    {

        public abstract ReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation, int interval = 30000);
        public abstract RcSummaryBase CurrentRc();

        // Assumes the RcSummary is already created
        // Also assumes this is executed under the ActivationContext to which the summary belongs!!
        public abstract void EnqueueRcExecution(string summaryKey);


        internal Logger Logger { get; }

        // Keeps track of cached summaries across an entire silo
        // , i.e. this is state that will be accessed concurrently by multiple Grains!
        // Maps a method's FullMethodKey() -> SummaryCache
        // FullMethodKey = "InterfaceId.MethodId[Arguments]"
        ConcurrentDictionary<string, RcCache> CacheMap;

        public RcManagerBase()
        {
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            Logger = LogManager.GetLogger("RcManager");
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
            var Timeout = this.CurrentRc().GetTimeout();
            var ncache = new RcCache<T>(this, Key, grain, request, options, Timeout);
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
                threadSafeRetrieval = cache.GetEnumeratorAsync(DependingRcSummary, out EnumAsync, Timeout);

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
                    grain.InitiateQuery<T>(request, Timeout, options);
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
                rcSummary.EnqueueExecution();
            }
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