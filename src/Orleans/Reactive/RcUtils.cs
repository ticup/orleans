using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Reactive
{
    public class RcUtils
    {
        // Temporary, should be replaced when this gets fixed:
        // https://github.com/dotnet/orleans/issues/1905
        public static object GetRawActivationKey(IAddressable Grain)
        {
            object rawKey = null;
            string strKey = null;

            try
            {
                rawKey = Grain.GetPrimaryKeyLong(out strKey);
                if ((long)rawKey == 0)
                {
                    rawKey = strKey;
                }
            }
            catch (InvalidOperationException)
            {
                rawKey = Grain.GetPrimaryKey(out strKey);
            }
            return rawKey;
        }
    }
}
