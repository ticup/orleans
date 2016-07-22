using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    /// <summary>
    /// Schedules execution of RcSummaries.
    /// Each activation has a worker, such that only one RcSummary is executed per activation.
    /// </summary>
    class RcSummaryWorker : BatchWorker
    {
        Dictionary<string, RcSummary> SummaryMap;
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
            // Pick a
            var CurrentRc = SummaryMap.First().Value;
            Current = CurrentRc;
            var Key = Current.GetLocalKey();

            var result = await Current.Execute();

            CurrentRc.SetResult(result);
            Current = null;

            TaskCompletionSource<object> Resolver;
            ResolverMap.TryGetValue(Key, out Resolver);
            if (Resolver == null)
            {
                throw new Runtime.OrleansException("Illegal state");
            }

            // TODO: is this the right place to send the messages?

            // Resolve promise for this work and remove it
            lock (this)
            {
                Resolver.SetResult(result);
                SummaryMap.Remove(Key);
                ResolverMap.Remove(Key);
            }
            
            var Messages = CurrentRc.GetPushMessages();
            foreach (var msg in Messages)
            {
                RuntimeClient.Current.SendPushMessage(msg);
            }

            //new TaskCompletionSource<TResult>();
            //var ctx = RuntimeContext.Current;
            //var context = new SchedulingContext(targetActivation);            
            //MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedOk(message);
            //Scheduler.QueueWorkItem(new InvokeWorkItem(targetActivation, message, context), context);
            //TaskScheduler.
            //Entry.Value.
            //Entry.Value.Execute
        }

        public Task<object> EnqueueSummary(RcSummary summary, TaskCompletionSource<object> resolver = null)
        {
            var Key = summary.GetLocalKey();

            lock (this)
            {
                var Exists = SummaryMap.ContainsKey(Key);
                TaskCompletionSource<object> Resolver;

                // This Summary is already schedule for execution, return its promise
                if (Exists)
                {
                    ResolverMap.TryGetValue(Key, out Resolver);
                    if (resolver != null)
                    {
                        Resolver.Task.ContinueWith((t) => resolver.SetResult(t.Result));
                    }
                    return Resolver.Task;
                }

                // Add the Summary for execution, create a promise and notify the worker
              
                Resolver = resolver == null ? new TaskCompletionSource<object>() : resolver;
                SummaryMap.Add(Key, summary);
                ResolverMap.Add(Key, Resolver);
                Notify();
                return Resolver.Task;
            }
        }
    }
}
