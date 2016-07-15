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

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var ReactComp = await grain.MyReactiveComp("stao erom tae");
            ReactComp.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            Assert.Equal(result, "foo");

            await grain.SetString("bar");

            var result2 = await ReactComp.OnUpdateAsync();
            Assert.Equal(result2, "bar");
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate()
        {

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var ReactComp = await grain.MyReactiveComp("stao erom tae");
            ReactComp.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            Assert.Equal(result, "foo");

            var task = ReactComp.OnUpdateAsync();

            await grain.SetString("bar");

            var result2 = await task;
            Assert.Equal(result2, "bar");
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate2()
        {

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var ReactComp = await grain.MyReactiveComp("stao erom tae");
            ReactComp.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            Assert.Equal(result, "foo");

            grain.SetString("bar");

            var result2 = await ReactComp.OnUpdateAsync(); ;
            Assert.Equal(result2, "bar");
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task DontPropagateWhenNoChange()
        {

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var ReactComp = await grain.MyReactiveComp("stao erom tae");
            ReactComp.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            Assert.Equal(result, "foo");

            await grain.SetString("foo");

            var task = ReactComp.OnUpdateAsync();

            await grain.SetString("bar");
            var result2 = await task;
            Assert.Equal(result2, "bar");

        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleSameComputation()
        {

            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var ReactComp = await grain.MyReactiveComp("stao erom tae");
            var ReactComp2 = await grain.MyReactiveComp("stao erom tae");
            ReactComp.KeepAlive();
            ReactComp2.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            var result2 = await ReactComp2.OnUpdateAsync();
            Assert.Equal(result, "foo");
            Assert.Equal(result2, "foo");

            await grain.SetString("bar");

            var result3 = await ReactComp.OnUpdateAsync();
            var result4 = await ReactComp2.OnUpdateAsync();
            Assert.Equal(result3, "bar");
            Assert.Equal(result4, "bar");
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultiLayeredComputation()
        {
            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(0);

            var grain1 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(0);
            var grain2 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(1);
            var grain3 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(2);

            await grain1.SetValue("Hello");
            await grain2.SetValue("my");
            await grain3.SetValue("lord!");

            await grain.SetGrains(new List<IMyOtherReactiveGrain> { grain1, grain2, grain3 });

            var ReactComp = await grain.MyLayeredComputation();
            ReactComp.KeepAlive();

            var result = await ReactComp.OnUpdateAsync();
            Assert.Equal(result, "Hello my lord!");

            await grain3.SetValue("lady!");
            var result2 = await ReactComp.OnUpdateAsync();
            Assert.Equal(result2, "Hello my lady!");
        }
    }
}
