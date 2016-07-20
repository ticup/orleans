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
    using System.Collections.Generic;

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
            options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose3;
            options.ClusterConfiguration.Defaults.TraceToConsole = true;
            //options.ClusterConfiguration.Globals.RegisterBootstrapProvider<SetInterceptorBootstrapProvider>(
            //    "SetInterceptorBootstrapProvider");
            return new TestCluster(options);
        }


        public void Dispose()
        {
            // clean up.
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncAfterUpdate()
        {

            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.OnUpdateAsyncAfterUpdate();
            
        }

        //[Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        //public async Task OnUpdateAsyncBeforeUpdate()
        //{

        //    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);

        //    var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
        //    ReactComp.KeepAlive();
        //    var It = ReactComp.GetAsyncEnumerator();

        //    var result = await It.OnUpdateAsync();
        //    Assert.Equal(result, "foo");

        //    var task = It.OnUpdateAsync();

        //    await grain.SetValue("bar");

        //    var result2 = await task;
        //    Assert.Equal(result2, "bar");
        //}

        //[Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        //public async Task OnUpdateAsyncBeforeUpdate2()
        //{

        //    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);

        //    var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
        //    var It = ReactComp.GetAsyncEnumerator();
        //    ReactComp.KeepAlive();

        //    var result = await It.OnUpdateAsync();
        //    Assert.Equal(result, "foo");

        //    grain.SetValue("bar");

        //    var result2 = await It.OnUpdateAsync();
        //    Assert.Equal(result2, "bar");
        //}

        //[Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        //public async Task DontPropagateWhenNoChange()
        //{

        //    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);

        //    var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
        //    ReactComp.KeepAlive();
        //    var It = ReactComp.GetAsyncEnumerator();

        //    var result = await It.OnUpdateAsync();
        //    Assert.Equal(result, "foo");

        //    await grain.SetValue("foo");

        //    var task = It.OnUpdateAsync();

        //    await grain.SetValue("bar");
        //    var result2 = await task;
        //    Assert.Equal(result2, "bar");

        //}


        //[Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        //public async Task MultipleIteratorsSameComputation()
        //{

        //    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);

        //    var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
        //    ReactComp.KeepAlive();

        //    var It = ReactComp.GetAsyncEnumerator();
        //    var It2 = ReactComp.GetAsyncEnumerator();


        //    var result = await It.OnUpdateAsync();
        //    var result2 = await It.OnUpdateAsync();
        //    Assert.Equal(result, "foo");
        //    Assert.Equal(result2, "foo");

        //    await grain.SetValue("bar");

        //    var result3 = await It.OnUpdateAsync();
        //    var result4 = await It2.OnUpdateAsync();
        //    Assert.Equal(result3, "bar");
        //    Assert.Equal(result4, "bar");
        //}

        //[Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        //public async Task MultiLayeredComputation()
        //{
        //    var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

           

        //    var grain1 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);
        //    var grain2 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(1);
        //    var grain3 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(2);

        //    await grain1.SetValue("Hello");
        //    await grain2.SetValue("my");
        //    await grain3.SetValue("lord!");

        //    await grain.SetGrains(new List<IMyOtherReactiveGrain> { grain1, grain2, grain3 });


        //    var ReactComp = GrainFactory.ReactiveComputation(() => grain.MyLayeredComputation());
        //    ReactComp.KeepAlive();
        //    var It = ReactComp.GetAsyncEnumerator();

        //    var result = await It.OnUpdateAsync();
        //    Assert.Equal(result, "Hello my lord!");

        //    await grain3.SetValue("lady!");
        //    var result2 = await It.OnUpdateAsync();
        //    Assert.Equal(result2, "Hello my lady!");
        //}
    }

    public class ReactiveGrainTestsGrain : Grain, IReactiveGrainTestsGrain
    {
        public async Task OnUpdateAsyncAfterUpdate()
        {
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);

            var Rc = await GrainFactory.ReactiveComputation(() =>
            {
                return grain.GetValue();
            });

            //Rc.KeepAlive();


            var It = Rc.GetAsyncEnumerator();

            var result = await It.OnUpdateAsync();
            Assert.Equal(result, "foo");

            await grain.SetValue("bar");

            var result2 = await It.OnUpdateAsync();
            Assert.Equal(result2, "bar");
        }
    }

}
