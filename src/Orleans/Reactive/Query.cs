using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    public delegate Task<T> InitiateQuery<T>(int interval, int timeout);

    public interface Query
    {
        string GetKey();
    }

    public class Query<TResult> : Query
    {
        GrainReference GrainReference;
        int InterfaceId;
        int MethodId;
        object[] Arguments;
        InvokeMethodOptions InvokeOptions;
        int KeepAliveInterval;
        int KeepAliveTimeout;

        public TResult Result { get; }
        Task<TResult> UpdateTask;
        CancellationTokenSource CancellationTokenSource;

        PullDependency[] PullsFrom;
        PushDependency[] PushedTo;

        InitiateQuery<TResult> InitiateQuery;
        Action KeepAliveAction;

        public Query(TResult result)
        {
            Result = result;
        }

        public Query(GrainReference grain, InvokeMethodRequest request, InitiateQuery<TResult> initiate)
        {
            GrainReference = grain;
            InterfaceId = request.InterfaceId;
            MethodId = request.MethodId;
            Arguments = request.Arguments;
            InitiateQuery = initiate;
            //KeepAliveAction = keepAlive;
            ResetUpdateTask();
        }

        // TODO: StringBuilder?
        public string GetKey()
        {
            return InterfaceId + "." + MethodId + "(" + Utils.EnumerableToString(Arguments) + ")";
        }

        private void ResetUpdateTask()
        {
            CancellationTokenSource = new CancellationTokenSource();
            UpdateTask = new Task<TResult>(() => Result, CancellationTokenSource.Token);
        }


        #region user interface
        public static Query<TResult> FromResult(TResult result)
        {
            return new Query<TResult>(result);
        }

        public async void KeepAlive(int interval = 5000, int timeout = 0)
        {
            KeepAliveInterval = interval;
            KeepAliveTimeout = (timeout == 0 ) ? interval * 2 : timeout;
            UpdateTask = InitiateQuery(KeepAliveInterval, KeepAliveTimeout);
        }

        public new void Cancel()
        {
            CancellationTokenSource.Cancel();
            ResetUpdateTask();
        }

        public new Task<TResult> OnUpdateAsync()
        {
            if (UpdateTask == null)
            {
                throw new Exception("Cannot call Query.OnUpdateAsync() before .KeepAlive()");
            }
            return UpdateTask;
        }
        #endregion

    }
}


