﻿using Orleans.CodeGeneration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Reactive
{



    public interface IReactiveComputation<TResult>
    {
        IResultEnumerator<TResult> GetResultEnumerator();


    }


   
}


