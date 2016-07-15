using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{

    public delegate void InitiateQuery<T>(int interval, int timeout, Query<T> query);

    public interface Query
    {
        int IdNumber { get; }
        void KeepAlive(int interval = 5000, int timeout = 0);
    }

    public class Query<TResult> : Query, IQueryCacheObserver
    {
        public int IdNumber { get; private set; }

        //GrainId GrainId;
        //int InterfaceId;
        //int MethodId;
        //object[] Arguments;
        //InvokeMethodOptions InvokeOptions;
        int KeepAliveInterval;
        int KeepAliveTimeout;

        public TResult Result { get; private set; }

        private bool PrevConsumed = true;
        private bool NextConsumed = false;

        Task<TResult> UpdateTask;
        CancellationTokenSource CancellationTokenSource;

      

        InitiateQuery<TResult> InitiateQuery;
        Action KeepAliveAction;

        // Only used temporarily to let the programmer construct a query returned from a query method,
        // using Query.FromResult(TResult)
        private Query(TResult result)
        {
            Result = result;
        }

        public Query(InitiateQuery<TResult> initiate)
        {
            IdNumber = RuntimeClient.Current.QueryManager.NewId();
            InitiateQuery = initiate;
            SetUpdateTask();
            //KeepAliveAction = keepAlive;
        }

        public string GetKey()
        {
            return IdNumber.ToString();
        }

        private void SetUpdateTask()
        {
            CancellationTokenSource = new CancellationTokenSource();
            UpdateTask = new Task<TResult>(() => {
                return Result;
            }, CancellationTokenSource.Token);
        }

        public async Task OnNext(object result)
        {

            Result = (TResult)result;
            if (!NextConsumed)
            {
                // S3: !NextConsumed && !PrevConsumed --> S3
                if (!PrevConsumed) return;

                // S1: !NextConsumed && PrevConsumed --> S3
                UpdateTask.Start();
                PrevConsumed = false;
                return;
            }

            // S2: NextConsumed && PrevConsumed --> S1
            UpdateTask.Start();
            SetUpdateTask();
            NextConsumed = false;
        }

        #region user interface
        public static Query<TResult> FromResult(TResult result)
        {
            return new Query<TResult>(result);
        }

        public void KeepAlive(int interval = 5000, int timeout = 0)
        {
            KeepAliveInterval = interval;
            KeepAliveTimeout = (timeout == 0 ) ? interval * 2 : timeout;
            // UpdateTask = new Task()
            try
            {
                InitiateQuery(KeepAliveInterval, KeepAliveTimeout, this);
                //    .ContinueWith((task) =>
                //{
                //    TriggerUpdate(task.Result);
                //});
            } catch( Exception e)
            {
                throw e;
            }
            
        }

        //public new void Cancel()
        //{
        //    CancellationTokenSource.Cancel();
        //    SetUpdateTask();
        //}

        public new Task<TResult> OnUpdateAsync()
        {
            //if (UpdateTask == null)
            //{
            //    throw new Exception("should never be null");
            //}
            if (!NextConsumed)
            {
                // S3: !NextConsumed && !PrevConsumed --> S1
                // => First consume the previous update, then setup to wait for the next one
                if (!PrevConsumed)
                {
                    var task = UpdateTask;
                    SetUpdateTask();
                    PrevConsumed = true;
                    return task;
                }

                // S1: !NextConsumed && PrevConsumed --> S2
                NextConsumed = true;
                return UpdateTask;
            }

            // S2: NextConsumed && PrevConsumed --> S2
            return UpdateTask;
        }
        #endregion

    }
}


