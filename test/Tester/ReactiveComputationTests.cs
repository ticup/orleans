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
    using System.Collections.Generic;
    using System.Linq;
    using Orleans.Runtime;


    /// <summary>
    /// White-box tests for ReactiveComputations.
    /// Debug using the logs in test/Tester/bin/Debug/logs
    /// If you want to trace reactive computation related messages:
    ///  i) remove all logs before running
    ///  ii) execute in logs dir: cat *.log | grep -E '(INFO)' | grep -E '(InsideRuntimeClient|GrainReference)'
    /// </summary>

    public class ReactiveComputationTests : OrleansTestingBase, IClassFixture<ReactiveComputationTests.Fixture>
    {

        public ReactiveComputationTests(ITestOutputHelper output) 
        {
            this.output = output;
        }
        private ITestOutputHelper output;

        private class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(2);
                options.ClusterConfiguration.AddMemoryStorageProvider("Default");
                //options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose3;
                options.ClusterConfiguration.Defaults.TraceToConsole = true;
                foreach (var o in options.ClusterConfiguration.Overrides)
                {
                    o.Value.TraceLevelOverrides.Add(new Tuple<string, Severity>("Runtime.RcManager", Severity.Verbose3));
                }
                return new TestCluster(options);
            }
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncAfterUpdate()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.OnUpdateAsyncAfterUpdate(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.OnUpdateAsyncBeforeUpdate(random.Next());
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate2()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.OnUpdateAsyncBeforeUpdate2(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task DontPropagateWhenNoChange()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.DontPropagateWhenNoChange(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleIteratorsSameComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.MultipleIteratorsSameComputation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultiLayeredComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.MultiLayeredComputation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task IteratorShouldOnlyReturnLatestValue()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.IteratorShouldOnlyReturnLatestValue(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task UseOfSameComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.MultipleComputationsUsingSameMethodSameActivation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleComputationsUsingSameMethodDifferentActivation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(0);
            await grain.MultipleComputationsUsingSameMethodDifferentActivation(random.Next());
        }

    }

    public class ReactiveGrainTestsGrain : Grain, IReactiveGrainTestsGrain
    {
        public async Task OnUpdateAsyncAfterUpdate(int randomoffset)
        {
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var Rc = GrainFactory.ReactiveComputation(() =>
            {
                return grain.GetValue();
            });

            var It = Rc.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "foo");

            await grain.SetValue("bar");
            result = await It.NextResultAsync();
            Assert.Equal(result, "bar");

            await grain.SetValue("bar2");
            result = await It.NextResultAsync();
            Assert.Equal(result, "bar2");
        }

        public async Task OnUpdateAsyncBeforeUpdate(int randomoffset)
        {

            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var Rc = GrainFactory.ReactiveComputation(() =>
            {
                return grain.GetValue();
            });

            var It = Rc.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "foo");

            var task = It.NextResultAsync();

            await grain.SetValue("bar");

            var result2 = await task;
            Assert.Equal(result2, "bar");
        }


        public async Task OnUpdateAsyncBeforeUpdate2(int randomoffset)
        {

            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
            var It = ReactComp.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "foo");

            grain.SetValue("bar");

            var result2 = await It.NextResultAsync();
            Assert.Equal(result2, "bar");
        }

        public async Task DontPropagateWhenNoChange(int randomoffset)
        {
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());
            var It = ReactComp.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "foo");

            await grain.SetValue("foo");

            var task = It.NextResultAsync();

            await grain.SetValue("bar");
            var result2 = await task;
            Assert.Equal(result2, "bar");

        }

        public async Task MultipleIteratorsSameComputation(int randomoffset)
        {

            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());

            var It = ReactComp.GetResultEnumerator();
            var It2 = ReactComp.GetResultEnumerator();


            var result = await It.NextResultAsync();
            var result2 = await It2.NextResultAsync();
            Assert.Equal(result, "foo");
            Assert.Equal(result2, "foo");

            await grain.SetValue("bar");

            var result3 = await It.NextResultAsync();
            var result4 = await It2.NextResultAsync();
            Assert.Equal(result3, "bar");
            Assert.Equal(result4, "bar");
        }

        public async Task IteratorShouldOnlyReturnLatestValue(int randomoffset)
        {

            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            var ReactComp = GrainFactory.ReactiveComputation(() => grain.GetValue());

            var It = ReactComp.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "foo");

            await grain.SetValue("bar");

            var result3 = await It.NextResultAsync();
            Assert.Equal(result3, "bar");

            var It2 = ReactComp.GetResultEnumerator();
            var result4 = await It2.NextResultAsync();
            Assert.Equal(result4, "bar");
        }



        public async Task MultiLayeredComputation(int randomoffset)
        {
            var grain = GrainFactory.GetGrain<IMyReactiveGrain>(randomoffset);



            var grain1 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + 0);
            var grain2 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + 1);
            var grain3 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + 2);

            await grain1.SetValue("Hello");
            await grain2.SetValue("my");
            await grain3.SetValue("lord!");

            await grain.SetGrains(new List<IMyOtherReactiveGrain> { grain1, grain2, grain3 });


            var ReactComp = GrainFactory.ReactiveComputation(() => grain.MyLayeredComputation());
            var It = ReactComp.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, "Hello my lord!");

            await grain3.SetValue("lady!");
            var result2 = await It.NextResultAsync();
            Assert.Equal(result2, "Hello my lady!");
        }


        public async Task MultipleComputationsUsingSameMethodSameActivation(int randomoffset)
        {
            int NumComputations = 10000;
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

            List<ReactiveComputation<string>> ReactComps = new List<ReactiveComputation<string>>();
            for (var i = 0; i < NumComputations; i++)
            {
                ReactComps.Add(GrainFactory.ReactiveComputation(() =>
                    grain.GetValue()
                ));
            }


            var Its = ReactComps.Select((Rc) => Rc.GetResultEnumerator()).ToList();

            // await all first results
            var Results1 = await Task.WhenAll(Its.Select(It =>
                It.NextResultAsync()
            ).ToList());

            foreach (var result1 in Results1)
            {
                Assert.Equal(result1, "foo");
            }

            // update the dependency
            await grain.SetValue("bar");

            // await all second results
            var Results2 = await Task.WhenAll(Its.Select(It => It.NextResultAsync()));

            foreach (var result2 in Results2)
            {
                Assert.Equal(result2, "bar");
            }
        }


        public async Task MultipleComputationsUsingSameMethodDifferentActivation(int randomoffset)
        {
            int NumComputations = 1000;

            List<ReactiveComputation<string>> ReactComps = new List<ReactiveComputation<string>>();
            for (var i = 0; i < NumComputations; i++)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + i);
                ReactComps.Add(GrainFactory.ReactiveComputation(() =>
                    grain.GetValue()));
            }


            var Its = ReactComps.Select((Rc) => Rc.GetResultEnumerator()).ToList();
            var Results1 = await Task.WhenAll(Its.Select(It => It.NextResultAsync()));
            foreach (var result1 in Results1)
            {
                Assert.Equal(result1, "foo");
            }

            for (var j = 0; j < NumComputations; j++)
            {
                await GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + j).SetValue("bar" + j);
            }

            var Results2 = await Task.WhenAll(Its.Select(It => It.NextResultAsync()));


            var k = 0;
            foreach (var result2 in Results2)
            {
                Assert.Equal(result2, "bar" + k++);
            }
        }

    }

}
