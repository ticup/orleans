using Orleans.CodeGeneration;
using Orleans.Reactive;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Reactive
{
    delegate Task<T> InitiateRcRequest<T>(ReactiveComputation<T> ReactComp, InvokeMethodRequest request, int interval, int timeout, InvokeMethodOptions options);

    internal class InsideRcManager : SystemTarget, IRcManager, RcManager
    {
        public static InsideRcManager CreateRcManager(Silo silo)
        {
            return new InsideRcManager(silo);
        }

        private Silo silo;
        internal Logger Logger { get; }

        bool IsPropagator;

        public InsideRcManager(Silo silo) : base(Constants.ReactiveCacheManagerId, silo.SiloAddress)
        {
            this.silo = silo;
            CacheMap = new ConcurrentDictionary<string, RcCache>();
            Logger = LogManager.GetLogger("RcManager");
            IsPropagator = false;
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
        internal ReactiveComputation<T> CreateReactiveComputation<T>(Func<Task<T>> computation)
        {
            var localKey = Guid.NewGuid();
            var SummaryMap = GetCurrentSummaryMap();
            var rc = new ReactiveComputation<T>(() => {
                RcSummaryBase disposed;
                SummaryMap.TryRemove(localKey.ToString(), out disposed);
                disposed.Dispose();
            });
            var RcSummary = new RcRootSummary<T>(localKey, computation, rc, 5000);
            var success = SummaryMap.TryAdd(localKey.ToString(), RcSummary);
            if (!success)
            {
                throw new OrleansException("Illegal State");
            }
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
        public async Task<T> ReuseOrRetrieveRcResult<T>(GrainId dependentGrain, GrainReference grain, InvokeMethodRequest request, InvokeMethodOptions options)
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
        public RcSummaryBase CurrentRc()
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


        public Task<bool> UpdateSummaryResult(string cacheMapKey, byte[] result, Exception exception)
        {
            RcCache Cache;
            if (! CacheMap.TryGetValue(cacheMapKey, out Cache))
                return Task.FromResult(false);
            Cache.OnNext(result, exception);
            return Task.FromResult(true);
        }

        public void RemoveCache(string Key)
        {
            RcCache Cache;
            CacheMap.TryRemove(Key, out Cache);
        }

    #endregion


    #region Summary API
        public ConcurrentDictionary<string, RcSummaryBase> GetCurrentSummaryMap()
        {
            return ((ActivationData)RuntimeClient.Current.CurrentActivationData).RcSummaryMap;
        }

    /// <summary>
    /// Reschedules calculation of the reactive computations of the current activation.
    /// </summary>
        public void RecomputeSummaries()
        {
            if (IsPropagator)
            {
                foreach (var q in GetCurrentSummaryMap().Values)
                {
                    q.EnqueueExecution();
                }
            }
            
        }


        /// <summary>
        /// Gets the <see cref="RcSummary"/> identified by the local key and current activation
        /// </summary>
        /// <param name="localKey"><see cref="RcSummary.GetLocalKey()"/></param>
        /// <returns></returns>
        public RcSummaryBase GetSummary(string localKey)
        {
            RcSummaryBase RcSummary;
            var SummaryMap = GetCurrentSummaryMap();
            SummaryMap.TryGetValue(localKey, out RcSummary);
            return RcSummary;
        }


        /// <summary>
        /// Concurrently gets or creates a <see cref="RcSummary"/> for given activation and request.
        /// </summary>
        public void CreateAndStartSummary<T>(object activationKey, IAddressable target, InvokeMethodRequest request, IGrainMethodInvoker invoker, int timeout, Message message, bool isRoot)
        {
            RcSummaryBase RcSummary;
            var SummaryMap = GetCurrentSummaryMap();
            var SummaryKey = GetMethodAndArgsKey(request);

            var NewRcSummary = new RcSummary<T>(activationKey, request, target, invoker, message.SendingAddress, timeout, this);

            var threadSafeRetrieval = false;
            bool existed;

            // We need to retrieve the summary and add the dependency under the lock of the summary
            // in order to prevent interleaving with removal of the summary from the SummaryMap.
            do
            {
                RcSummary = SummaryMap.GetOrAdd(SummaryKey, NewRcSummary);
                existed = RcSummary != NewRcSummary;

                threadSafeRetrieval = ((RcSummary)RcSummary).AddPushDependency(message, timeout);
            } while (!threadSafeRetrieval);
            
            if (!existed)
            {
                RcSummary.EnqueueExecution();
            }
            IsPropagator = true;
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