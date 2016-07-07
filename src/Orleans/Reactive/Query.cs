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
        void KeepAlive(int interval = 5000, int timeout = 0);
    }

    public class Query<TResult> : Query
    {
        //GrainReference GrainReference;
        int InterfaceId;
        int MethodId;
        object[] Arguments;
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

        public Query(TResult result)
        {
            Result = result;
        }

        public Query(InitiateQuery<TResult> initiate)
        {
            InitiateQuery = initiate;
            SetUpdateTask();
            //KeepAliveAction = keepAlive;
        }

        // TODO: StringBuilder?
        public string GetKey()
        {
            return QueryManager.GetKey(InterfaceId, MethodId, Arguments);
        }

        private void SetUpdateTask()
        {
            CancellationTokenSource = new CancellationTokenSource();
            UpdateTask = new Task<TResult>(() => {
                return Result;
            }, CancellationTokenSource.Token);
        }

        public void TriggerUpdate(TResult result)
        {
            Result = result;
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
            InitiateQuery(KeepAliveInterval, KeepAliveTimeout).ContinueWith((task) =>
            {
                TriggerUpdate(task.Result);
            });
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


