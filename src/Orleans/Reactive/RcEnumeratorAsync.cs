using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{
    public class RcEnumeratorAsync<TResult>
    {


        public TResult Result { get; private set; }

        private bool PrevConsumed = true;
        private bool NextConsumed = false;


        Task<TResult> UpdateTask;
        CancellationTokenSource CancellationTokenSource;



        public RcEnumeratorAsync()
        {
            SetUpdateTask();

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


        public Task<TResult> OnUpdateAsync()
        {
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

    }
}
