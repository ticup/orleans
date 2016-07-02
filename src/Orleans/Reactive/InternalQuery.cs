using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public interface InternalQuery : Query
    {
        string GetKey();
    }

    public class InternalQuery<TResult> : Query<TResult>, InternalQuery
    {
        GrainReference GrainReference;
        int InterfaceId;
        int MethodId;
        object[] Arguments;
        InvokeMethodOptions InvokeOptions;

        Task<TResult> UpdateTask;
        CancellationTokenSource CancellationTokenSource;

        public InternalQuery(GrainReference grain, int interfaceId, int methodId, object[] arguments, InvokeMethodOptions invokeOptions = InvokeMethodOptions.None)
        {
            GrainReference = grain;
            InterfaceId = interfaceId;
            MethodId = methodId;
            Arguments = arguments;
            InvokeOptions = invokeOptions;
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
        public new Query<TResult> KeepAlive()
        {
           var request = new InvokeMethodRequest(InterfaceId, MethodId, Arguments);

            //if (IsUnordered)
            //    options |= InvokeMethodOptions.Unordered;

            //Task<object> resultTask = InvokeMethod_Impl(request, null, options);

            //}

           

            return this;
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

        public new Task<TResult> React()
        {
            //RuntimeClient.Current.
            return Task.FromResult(Result);
        }

        #endregion

    }
}


