using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    class PushResultTo
    {
        public List<Message> Targets;
        public object Result;
        public int InterfaceId;
        public int MethodId;
        public object[] Arguments;
    }
}
