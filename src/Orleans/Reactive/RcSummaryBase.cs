using Orleans.Runtime;
using Orleans.Runtime.Reactive;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Reactive
{

    interface RcSummaryBase: IDisposable
    {
        string GetFullKey();
        string GetLocalKey();
        #region Cache Dependency Tracking
        /// <summary>
        /// Returns true if this summary has a dependency on given summary.
        /// </summary>
        /// <param name="FullMethodKey">A <see cref="RcSummary.GetFullKey()"/> that Identifies a <see cref="RcSummary"/></param>
        /// <returns></returns>
        bool HasDependencyOn(string FullMethodKey);

        void KeepDependencyAlive(string fullMethodKey);


        /// <summary>
        /// Sets all the <see cref="RcCacheDependency"/> their <see cref="RcCacheDependency.IsAlive"/> flag to false.
        /// By executing the summary afterwards, all the dependencies that this summary re-used
        /// will have their IsAlive flag set to true again via <see cref="KeepDependencyAlive(string)"/>.
        /// Synergizes with <see cref="CleanupInvalidDependencies"/> in <see cref="RcSummaryWorker.Work"/>
        /// </summary>
        void ResetDependencies();

        /// <summary>
        /// Remove all the <see cref="RcCacheDependency"/> on which this summary no longer depends by looking at the <see cref="RcCacheDependency.IsAlive"/> flag.
        /// Synergizes with <see cref="ResetDependencies"/> in <see cref="RcSummaryWorker.Work"/>
        /// </summary>
        void CleanupInvalidDependencies();

        /// <summary>
        /// Add given cache as a dependency for this summary.
        /// </summary>
        /// <param name="fullMethodKey">The <see cref="GetFullKey"/> that identifies the summary of the <param name="rcCache"></param>
        void AddCacheDependency(string FullMethodKey, RcCache rcCache);
        #endregion
        int GetTimeout();
        Task<object> Execute();
        void EnqueueExecution();
        Task<bool> UpdateResult(object result, Exception exceptionResult);

    }

    class RcCacheDependency
    {
        public RcCache Cache;
        public bool IsAlive;

        public RcCacheDependency(RcCache cache)
        {
            Cache = cache;
            IsAlive = true;
        }
    }

    enum RcSummaryState
    {
        NotYetComputed,
        HasResult,
        Exception
    }

    abstract class RcSummaryBase<TResult>: RcSummaryBase
    {

        public abstract string GetFullKey();
        public abstract string GetLocalKey();
        public abstract Task OnChange();
        public abstract Task<object> Execute();


        protected RcSummaryState State;

        public TResult Result { get; private set; }

        public byte[] SerializedResult { get; private set; }

        public Exception ExceptionResult { get; private set; }

        private Dictionary<string, RcCacheDependency> CacheDependencies = new Dictionary<string, RcCacheDependency>();

        private int Timeout;


        public RcSummaryBase(int timeout) {
            State = RcSummaryState.NotYetComputed;
            Timeout = timeout;
        }

        public void EnqueueExecution()
        {
            RuntimeClient.Current.RcManager.EnqueueRcExecution(this.GetLocalKey());
        }


        /// <summary>
        /// Update the state of the summary and notify dependents if it is different from the previous state.
        /// </summary>
        /// <param name="result">the latest result, if there is no exception</param>
        /// <param name="exceptionResult">the latest result</param>
        /// <returns>true if the result of the summary changed, or false if it is the same</returns>
        public virtual async Task<bool> UpdateResult(object result, Exception exceptionResult)
        {
            if (exceptionResult == null)
            {

                var tresult = (TResult)result;

                // serialize the result into a byte array
                BinaryTokenStreamWriter stream = new BinaryTokenStreamWriter();
                Serialization.SerializationManager.Serialize(tresult, stream);
                var serializedresult = stream.ToByteArray();

                if (SerializedResult != null && SerializationManager.CompareBytes(SerializedResult, serializedresult))
                    return false;

                // store latest result
                State = RcSummaryState.HasResult;
                Result = tresult;
                SerializedResult = serializedresult;
                ExceptionResult = null;
            }
            else
            {
                State = RcSummaryState.Exception;
                Result = default(TResult);
                SerializedResult = null;
                ExceptionResult = exceptionResult;
            }

            await OnChange();

            return true;
        }



        public void CleanupInvalidDependencies()
        {
            var ToRemove = CacheDependencies.Where((kvp) => !kvp.Value.IsAlive).ToList();
            foreach (var kvp in ToRemove)
            {
                CacheDependencies.Remove(kvp.Key);
                kvp.Value.Cache.RemoveDependencyFor(this);
            }
        }


        public void Dispose()
        {
            lock (this)
            {
                var dependencies = CacheDependencies;
                CacheDependencies = new Dictionary<string, RcCacheDependency>();

                foreach (var cd in dependencies)
                {
                    cd.Value.Cache.RemoveDependencyFor(this);
                }
            }
        }


        #region Cache Dependency Tracking
        public bool HasDependencyOn(string fullMethodKey)
        {
            return CacheDependencies.ContainsKey(fullMethodKey);
        }

        public void KeepDependencyAlive(string fullMethodKey)
        {
            RcCacheDependency Dep;
            CacheDependencies.TryGetValue(fullMethodKey, out Dep);
            if (Dep == null)
            {
                throw new Exception("illegal state");
            }
            Dep.IsAlive = true;
        }

        public void AddCacheDependency(string FullMethodKey, RcCache rcCache)
        {
            CacheDependencies.Add(FullMethodKey, new RcCacheDependency(rcCache));
        }

        public void ResetDependencies()
        {
            foreach (var dep in CacheDependencies)
            {
                dep.Value.IsAlive = false;
            }
        }
        #endregion
        public int GetTimeout()
        {
            return Timeout;
        }
    }
}
