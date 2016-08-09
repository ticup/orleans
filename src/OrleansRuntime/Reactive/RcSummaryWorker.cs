using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    /// <summary>
    /// Schedules execution of <see cref="RcSummary"/>.
    /// Each activation has a worker, such that only one RcSummary is executed per activation.
    /// </summary>
    /// <remarks>
    /// The workers are currently managed in the <see cref="RcManager.WorkerMap"/> per activation.
    /// They are created on a per-request basis.
    /// </remarks>
    class RcSummaryWorker : BatchWorker
    {
        /// <summary>
        /// Maps <see cref="RcSummary.GetLocalKey()"/> to <see cref="RcSummary"/>
        /// </summary>
        Dictionary<string, RcSummary> SummaryMap;
        /// <summary>
        /// Maps <see cref="RcSummary.GetLocalKey()"/> to <see cref="TaskCompletionSource{Object}"/>
        /// </summary>
        /// <remarks>
        /// The invariant should hold that whenever there is an entry in the SummaryMap for a given key, there will be an entry in the ResolverMap
        /// </remarks>
        Dictionary<string, TaskCompletionSource<object>> ResolverMap;

        GrainId grainId;
        RcManager rcManager;

        public RcSummary Current { get; private set; }

        public RcSummaryWorker(GrainId grainId, RcManager rcManager)
        {
            this.grainId = grainId;
            this.rcManager = rcManager;
            SummaryMap = new Dictionary<string, RcSummary>();
            ResolverMap = new Dictionary<string, TaskCompletionSource<object>>();
        }

        protected override async Task Work()
        {
            if (Current != null)
            {
                throw new Runtime.OrleansException("illegal state");
            }

            string Key;
            TaskCompletionSource<object> Resolver;

            lock (this)
            {
                // Pick a summary to be executed and remove it
                Current = SummaryMap.First().Value;
                Key = Current.GetLocalKey();
                Resolver = GetResolver(Key);

                SummaryMap.Remove(Key);
                ResolverMap.Remove(Key);
            }

            var context = RuntimeContext.CurrentActivationContext.CreateReactive();

            await RuntimeClient.Current.ExecAsync(async () => {

                // Execute the computation
                object result = null;
                Exception exception_result = null;
                try
                {
                    result = await Current.Execute();
                }
                catch (Exception e)
                {
                    exception_result = e;
                }
                // todo: propagate exception results the same way as normal results

                // Set the result in the summary
                var changed = Current.UpdateResult(result);

                // Resolve promise for this work
                Resolver.SetResult(result);

                // If result has changed, notify all caches
                if (changed)
                {
                    var tasks = new List<Task>();
                    foreach (var kvp in Current.GetDependentSilos().ToList())
                        tasks.Add(PushToSilo(kvp.Key, kvp.Value));
                    await Task.WhenAll(tasks);
                }

                Current = null;

            }, context, "Reactive Computation");

        private async Task PushToSilo(SiloAddress silo, PushDependency dependency)
        {
            // get the Rc Manager System Target on the remote grain
            var rcmgr = InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<IRcManager>(Constants.ReactiveCacheManagerId, silo);

            bool silo_remains_dependent = false;
            try
            {
                // send a push message to the rc manager on the remote grain
                silo_remains_dependent = await rcmgr.UpdateSummaryResult(Current.GetCacheMapKey(), Current.SerializedResult);
            }
            catch (Exception e)
            {
                rcManager.Logger.Warn(ErrorCode.ReactiveCaches_PushFailure, "Caught exception when updating summary result for {0} on silo {1}: {2}", grainId, silo, e);
            }

            if (!silo_remains_dependent)
            {
                Current.RemoveDependentSilo(silo);
            }
        }

        public Task<object> EnqueueSummary(RcSummary summary, TaskCompletionSource<object> resolver = null)
        {
            var Key = summary.GetLocalKey();

            lock (this)
            {
                var Exists = SummaryMap.ContainsKey(Key);
                TaskCompletionSource<object> Resolver;

                // This Summary is already scheduled for execution, return its promise
                if (Exists)
                {
                    Resolver = GetResolver(Key);
                    if (resolver != null)
                    {
                        Resolver.Task.ContinueWith((t) => resolver.TrySetResult(t.Result));
                    }
                    return Resolver.Task;
                }

                // Otherwise, add the Summary for execution, create a promise and notify the worker
                Resolver = resolver == null ? new TaskCompletionSource<object>() : resolver;
                SummaryMap.Add(Key, summary);
                ResolverMap.Add(Key, Resolver);
                Notify();
                return Resolver.Task;
            }
        }

        private TaskCompletionSource<object> GetResolver(string Key)
        {
            TaskCompletionSource<object> Resolver;
            ResolverMap.TryGetValue(Key, out Resolver);
            if (Resolver == null)
            {
                throw new Runtime.OrleansException("Illegal state");
            }

            return Resolver;
        }
    }
}
