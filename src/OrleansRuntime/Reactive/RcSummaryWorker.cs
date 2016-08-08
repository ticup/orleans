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

        public RcSummary Current { get; private set; }

        public RcSummaryWorker()
        {
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

            // Execute the computation
            var result = await Current.Execute();

            // Set the result in the summary
            Current.SetResult(result);


            // TODO: is this the right place to send the messages?
            var Messages = Current.GetPushMessages();
            foreach (var msg in Messages)
            {
                RuntimeClient.Current.SendPushMessage(msg);
            }

            // Resolve promise for this work and remove it
            Resolver.SetResult(result);

            Current = null;
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
