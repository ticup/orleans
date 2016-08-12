using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler
{
    class ActivationTask
    {
        public Task Task;
        public bool IsReactive;

        public ActivationTask(Task task, bool isReactive)
        {
            Task = task;
            IsReactive = isReactive;
        }
    }
}
