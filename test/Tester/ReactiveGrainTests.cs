namespace Tester
{
    using System.Threading.Tasks;
    using Orleans;
    using UnitTests.GrainInterfaces;
    using UnitTests.Tester;
    using Xunit;
    using Xunit.Abstractions;
    using System;
    using Orleans.TestingHost;
    using Orleans.Runtime.Configuration;
    using Orleans.Providers;
    using System.Linq;
    public class ReactiveGrainTests : TestClusterPerTest, IDisposable
    {

        public static ITestOutputHelper TestOutput;

        public ReactiveGrainTests(ITestOutputHelper output)
        {
            TestOutput = output;
        }

        public override TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions(2);
            options.ClusterConfiguration.AddMemoryStorageProvider("Default");
            //options.ClusterConfiguration.Globals.RegisterBootstrapProvider<SetInterceptorBootstrapProvider>(
            //    "SetInterceptorBootstrapProvider");
            return new TestCluster(options);
        }
 

        public void Dispose()
        {
            // clean up.
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task GrainMethodInterceptionTest()
        {

            GrainClient.ClientInvokeCallback = (request, g) =>
            {
                Console.WriteLine("intercepting at the client: " + request.ToString());
            };

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var query = await grain.MyQuery("stao erom tae");
            query.KeepAlive();

            var result = await query.OnUpdateAsync();
            Assert.Equal(result, "foo");

            await grain.SetString("bar");

            var result2 = await query.OnUpdateAsync();
            Assert.Equal(result2, "bar");
        }
    }

    //public class SetInterceptorBootstrapProvider : IBootstrapProvider
    //{
    //    public string Name { get; private set; }

    //    public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
    //    {
    //        providerRuntime.SetInvokeInterceptor((method, request, grain, invoker) =>
    //        {
    //            if (method != null)
    //            {
    //                Console.WriteLine("intercepted method at server");
    //                //providerRuntime.GetLogger("test").Info("intercepting message");
    //            }
    //            return invoker.Invoke(grain, request);
    //        });

    //        return Task.FromResult(0);
    //    }

    //    public Task Close()
    //    {
    //        return Task.FromResult(0);
    //    }
    //}
}
