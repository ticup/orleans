using Orleans.Runtime.Reactive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Reactive
{
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
}
