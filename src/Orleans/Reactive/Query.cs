using Orleans.CodeGeneration;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{
    public interface Query {
    }
    public class Query<TResult> : Query
    {
        protected TResult Result;


        public Query() { }
        public Query(TResult result) {
            Result = result;
        }

        #region user interface
        public static Query<TResult> FromResult(TResult result)
        {
            return new Query<TResult>(result);
        }

        public Query<TResult> KeepAlive()
        {
            throw new Exception("Should be implemented by InternalQuery");
        }

        public void Cancel()
        {
            throw new Exception("Should be implemented by InternalQuery");
        }

        public Task<TResult> OnUpdateAsync()
        {
            throw new Exception("Should be implemented by InternalQuery");
        }

        public Task<TResult> React()
        {
            throw new Exception("Should be implemented by InternalQuery");
        }


        #endregion
    }
}
