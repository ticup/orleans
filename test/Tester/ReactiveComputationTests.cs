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
    using Orleans.Reactive;

    /// <summary>
    /// White-box tests for ReactiveComputations.
    /// Debug using the logs in test/Tester/bin/Debug/logs
    /// If you want to trace reactive computation related messages:
    ///  i) remove all logs before running
    ///  ii) execute in logs dir: cat *.log | grep -E '(INFO|VERBOSE)' | grep -E '(InsideRuntimeClient|GrainReference|RcManager)' 
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
                options.ClientConfiguration.ReactiveComputationRefresh = TimeSpan.FromSeconds(3);
                options.ClusterConfiguration.Globals.ReactiveComputationRefresh = TimeSpan.FromSeconds(3);
                options.ClusterConfiguration.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose3;
                //options.ClientConfiguration.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose3;
                options.ClusterConfiguration.Defaults.TraceToConsole = true;
                options.ClientConfiguration.TraceLevelOverrides.Add(new Tuple<string, Severity>("Runtime.RcManager", Severity.Verbose3));
                options.ClientConfiguration.TraceLevelOverrides.Add(new Tuple<string, Severity>("RcManager", Severity.Verbose3));
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
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.OnUpdateAsyncAfterUpdate(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.OnUpdateAsyncBeforeUpdate(random.Next());
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task OnUpdateAsyncBeforeUpdate2()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.OnUpdateAsyncBeforeUpdate2(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task DontPropagateWhenNoChange()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.DontPropagateWhenNoChange(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task FilterIdenticalResults()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.FilterIdenticalResults(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleIteratorsSameComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.MultipleIteratorsSameComputation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultiLayeredComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.MultiLayeredComputation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task IteratorShouldOnlyReturnLatestValue()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.IteratorShouldOnlyReturnLatestValue(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task UseOfSameComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.MultipleComputationsUsingSameMethodSameActivation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleComputationsUsingSameMethodDifferentActivation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.MultipleComputationsUsingSameMethodDifferentActivation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task MultipleCallsFromSameComputation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.MultipleCallsFromSameComputation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task ExceptionPropagation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.ExceptionPropagation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task GrainKeyTypes()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.GrainKeyTypes(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task CacheDependencyInvalidation()
        {
            var grain = GrainFactory.GetGrain<IReactiveGrainTestsGrain>(random.Next());
            await grain.CacheDependencyInvalidation(random.Next());
        }

        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task ClientComputation()
        {
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(random.Next());

            var Rc = GrainFactory.StartReactiveComputation(() =>
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
            Rc.Dispose();
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task ClientMultipleComputationsAndIterators()
        {
            var NUM_COMPUTATIONS = 1000;
            var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(random.Next());

            var Rcs = new List<IReactiveComputation<string[]>>();

            for (var i = 0; i < NUM_COMPUTATIONS; i++)
            {
                Rcs.Add(GrainFactory.StartReactiveComputation(async () =>
                {
                    var res1 = await grain.GetValue(1);
                    var res2 = await grain.GetValue(1);
                    var res3 = await grain.GetValue(2);
                    return new[] { res1, res2, res3 };
                }));
            }

            var Its1 = Rcs.Select((rc) => rc.GetResultEnumerator()).ToList();
            var Its2 = Rcs.Select((rc) => rc.GetResultEnumerator()).ToList();


            var NextResults1 = await Task.WhenAll(Its1.Select((it) => it.NextResultAsync()).ToList());
            var NextResults2 = await Task.WhenAll(Its2.Select((it) => it.NextResultAsync()).ToList());

            foreach (var NextResult1 in NextResults1)
            {
                Assert.Equal(NextResult1, new[] { "foo", "foo", "foo" });
            }
            foreach (var NextResult2 in NextResults2)
            {
                Assert.Equal(NextResult2, new[] { "foo", "foo", "foo" });
            }

            await grain.SetValue("bar");

            // wait for all updates to propagate
            await Task.Delay(3000);

            NextResults1 = await Task.WhenAll(Its1.Select((it) => it.NextResultAsync()).ToList());
            NextResults2 = await Task.WhenAll(Its2.Select((it) => it.NextResultAsync()).ToList());

            foreach (var NextResult1 in NextResults1)
            {
                Assert.Equal(NextResult1, new string[] { "bar", "bar", "bar" });
            }
            foreach (var NextResult2 in NextResults2)
            {
                Assert.Equal(NextResult2, new[] { "bar", "bar", "bar" });
            }
            foreach (var Rc in Rcs)
            {
                Rc.Dispose();
            }
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task SubscriptionCleanup()
        {
            var r = random.Next();
            var grain1 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(r);
            var grain2 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(r + 1);

            var Rc = GrainFactory.StartReactiveComputation(async () =>
            {
                var res1 = await grain1.GetValue();
                var res2 = await grain2.GetValue();
                var res3 = "";
                if (res1 == "foo")
                {
                    res3 = await grain2.GetValue(2);
                }

                return new[] { res1, res2, res3 };
            });

            var It = Rc.GetResultEnumerator();

            var result = await It.NextResultAsync();
            Assert.Equal(result, new[] { "foo", "foo", "foo" });

            await grain1.SetValue("bar");
            result = await It.NextResultAsync();
            Assert.Equal(result, new[] { "bar", "foo", "" });

            await Task.Delay(TimeSpan.FromSeconds(10));
            // should have removed cache and summary
            //logger.Verbose("done waiting");

            await grain1.SetValue("foo");
            result = await It.NextResultAsync();
            Assert.Equal(result, new[] { "foo", "foo", "foo" });
            Rc.Dispose();
        }


        [Fact, TestCategory("Functional"), TestCategory("ReactiveGrain")]
        public async Task Chirper()
        {
            var user1 = GrainFactory.GetGrain<IChirperUserGrain>(random.Next().ToString());
            //

            var rcTimeline = GrainFactory.StartReactiveComputation(async () => await user1.GetTimeline(100));
            var rcFollowerList = GrainFactory.StartReactiveComputation(async () => await user1.GetFollowersList());

            var tIt = rcTimeline.GetResultEnumerator();
            var fIt = rcFollowerList.GetResultEnumerator();

            var timeline = await tIt.NextResultAsync();
            var followers = await fIt.NextResultAsync();

            Assert.Equal(timeline.Posts, new List<UserMessage>());
            Assert.Equal(followers, new List<string>());

            await user1.Follow("bar");

            followers = await fIt.NextResultAsync();
            Assert.Equal(followers, new List<string>{ "bar" });

            var user2 = GrainFactory.GetGrain<IChirperUserGrain>("bar");
            await user2.PostText("hello world!");

            timeline = await tIt.NextResultAsync();
            Assert.Equal(timeline.Posts[0].Text, "hello world!");


            //user2.GetFollowerList();
        }


        public class ReactiveGrainTestsGrain : Grain, IReactiveGrainTestsGrain
        {
            public async Task OnUpdateAsyncAfterUpdate(int randomoffset)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var Rc = GrainFactory.StartReactiveComputation(() =>
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
                Rc.Dispose();
            }

            public async Task OnUpdateAsyncBeforeUpdate(int randomoffset)
            {

                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var Rc = GrainFactory.StartReactiveComputation(() =>
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
                Rc.Dispose();
            }


            public async Task OnUpdateAsyncBeforeUpdate2(int randomoffset)
            {

                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var ReactComp = GrainFactory.StartReactiveComputation(() => grain.GetValue());
                var It = ReactComp.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                var task = grain.SetValue("bar");

                var result2 = await It.NextResultAsync();
                Assert.Equal(result2, "bar");
                ReactComp.Dispose();
            }

            public async Task DontPropagateWhenNoChange(int randomoffset)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var ReactComp = GrainFactory.StartReactiveComputation(() => grain.GetValue());
                var It = ReactComp.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                await grain.SetValue("foo");

                var task = It.NextResultAsync();

                await grain.SetValue("bar");
                var result2 = await task;
                Assert.Equal(result2, "bar");
                ReactComp.Dispose();

            }

            public async Task FilterIdenticalResults(int randomoffset)
            {
                try
                {
                    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);
                    await grain.SetValue("foo");

                    var ReactComp1 = GrainFactory.StartReactiveComputation(async () =>
                    {
                        var s = await grain.GetValue();
                        return s;
                    });
                    var ReactComp2 = GrainFactory.StartReactiveComputation(async () =>
                    {
                        var s = await grain.GetValue();
                        return s.Length;
                    });

                    // get first results
                    var It1 = ReactComp1.GetResultEnumerator();
                    var It2 = ReactComp2.GetResultEnumerator();
                    var a1 = It1.NextResultAsync();
                    var a2 = It2.NextResultAsync();
                    await Task.WhenAll(a1, a2);
                    Assert.Equal("foo", a1.Result);
                    Assert.Equal(3, a2.Result);

                    // no-op change
                    await grain.SetValue("foo");
                    await Task.Delay(10);
                    Assert.False(It1.NextResultIsReady);
                    Assert.False(It2.NextResultIsReady);

                    // change string but not length
                    await grain.SetValue("bar");
                    await Task.Delay(10);
                    Assert.True(It1.NextResultIsReady);
                    Assert.False(It2.NextResultIsReady);
                    ReactComp1.Dispose();
                    ReactComp2.Dispose();
                } catch (Exception e)
                {
                    throw e;
                }
            }

            public async Task MultipleIteratorsSameComputation(int randomoffset)
            {

                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var ReactComp = GrainFactory.StartReactiveComputation(() => grain.GetValue());

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
                ReactComp.Dispose();
            }

            public async Task IteratorShouldOnlyReturnLatestValue(int randomoffset)
            {

                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var ReactComp = GrainFactory.StartReactiveComputation(() => grain.GetValue());

                var It = ReactComp.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                await grain.SetValue("bar");

                var result3 = await It.NextResultAsync();
                Assert.Equal(result3, "bar");

                var It2 = ReactComp.GetResultEnumerator();
                var result4 = await It2.NextResultAsync();
                Assert.Equal(result4, "bar");
                ReactComp.Dispose();
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


                var ReactComp = GrainFactory.StartReactiveComputation(() => grain.MyLayeredComputation());
                var It = ReactComp.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "Hello my lord!");

                await grain3.SetValue("lady!");
                var result2 = await It.NextResultAsync();
                Assert.Equal(result2, "Hello my lady!");
                ReactComp.Dispose();
            }


            public async Task MultipleComputationsUsingSameMethodSameActivation(int randomoffset)
            {
                int NumComputations = 100;
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                List<IReactiveComputation<string>> ReactComps = new List<IReactiveComputation<string>>();
                for (var i = 0; i < NumComputations; i++)
                {
                    ReactComps.Add(GrainFactory.StartReactiveComputation(() =>
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

                foreach (var Rc in ReactComps)
                {
                    Rc.Dispose();
                }
            }


            public async Task MultipleComputationsUsingSameMethodDifferentActivation(int randomoffset)
            {
                int NumComputations = 1000;

                List<IReactiveComputation<string>> ReactComps = new List<IReactiveComputation<string>>();
                for (var i = 0; i < NumComputations; i++)
                {
                    var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + i);
                    ReactComps.Add(GrainFactory.StartReactiveComputation(() =>
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

                foreach (var Rc in ReactComps)
                {
                    Rc.Dispose();
                }
            }

            public async Task MultipleCallsFromSameComputation(int randomoffset)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                var Rc = GrainFactory.StartReactiveComputation(async () =>
                {
                    var res1 = await grain.GetValue();
                    var res2 = await grain.GetValue();
                    var res3 = await grain.GetValue(1);
                    return new[] { res1, res2, res3 };
                });

                var It = Rc.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, new[] { "foo", "foo", "foo" });

                await grain.SetValue("bar");
                await Task.Delay(1000);
                result = await It.NextResultAsync();
                Assert.Equal(result, new[] { "bar", "bar", "bar" });

                await grain.SetValue("bar2");
                await Task.Delay(1000);
                result = await It.NextResultAsync();
                Assert.Equal(result, new[] { "bar2", "bar2", "bar2" });
                Rc.Dispose();
            }

            public async Task ExceptionCatching(int randomoffset)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                await grain.SetValue("fault");

                var Rc = GrainFactory.StartReactiveComputation(async () =>
                {
                    var res1 = await grain.GetValue();
                    var res2 = await grain.FaultyMethod();
                    return res1;
                });

                var It = Rc.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                await grain.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grain.SetValue("fault");
                Exception e = await Assert.ThrowsAsync<Exception>(() => It.NextResultAsync());
                Assert.Equal(e.Message, "faulted");

                await grain.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();
            }

            public async Task ExceptionPropagation(int randomoffset)
            {
                var grain = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);

                // Catch the exception on .NextResulAsync()
                var Rc = GrainFactory.StartReactiveComputation(async () =>
                {
                    var res1 = await grain.GetValue();
                    var res2 = await grain.FaultyMethod();
                    return res1;
                });

                var It = Rc.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                await grain.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grain.SetValue("fault");
                await Task.Delay(1000);
                Exception e = await Assert.ThrowsAsync<Exception>(() => It.NextResultAsync());

                await grain.SetValue("success");
                await Task.Delay(1000);
                result = await It.NextResultAsync();
                Assert.Equal(result, "success");
                Rc.Dispose();

                // Catch the exception inside the computation
                var Rc1 = GrainFactory.StartReactiveComputation(async () =>
                {
                    await grain.GetValue();
                    try
                    {
                        await grain.FaultyMethod();
                        return false;
                    }
                    catch (Exception exc)
                    {
                        return true;
                    }
                });
                var It1 = Rc1.GetResultEnumerator();

                var result1 = await It1.NextResultAsync();
                Assert.Equal(result1, false);

                await grain.SetValue("fault");
                await Task.Delay(1000);
                result1 = await It1.NextResultAsync();
                Assert.Equal(result1, true);

                await grain.SetValue("success");
                await Task.Delay(1000);
                result1 = await It1.NextResultAsync();
                Assert.Equal(result1, false);
                Rc1.Dispose();
            }

            public async Task GrainKeyTypes(int randomoffset)
            {

                // GrainWithGuidCompoundKey
                IReactiveGrainGuidCompoundKey grainGuidCompoundKey = GrainFactory.GetGrain<IReactiveGrainGuidCompoundKey>(Guid.NewGuid(), "key extension", null);
                var Rc = GrainFactory.StartReactiveComputation(() =>
                    grainGuidCompoundKey.GetValue()
                );
                var It = Rc.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                await grainGuidCompoundKey.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grainGuidCompoundKey.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();

                // GrainWithGuidKey
                var grainGuidKey = GrainFactory.GetGrain<IReactiveGrainGuidKey>(Guid.NewGuid());
                Rc = GrainFactory.StartReactiveComputation(() =>
                    grainGuidKey.GetValue()
                );
                It = Rc.GetResultEnumerator();

                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                await grainGuidKey.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grainGuidKey.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();

                // GrainWithIntegerCompoundKey
                var grainIntegerCompoundKey = GrainFactory.GetGrain<IReactiveGrainIntegerCompoundKey>(random.Next(), "extension", null);
                Rc = GrainFactory.StartReactiveComputation(() =>
                    grainIntegerCompoundKey.GetValue()
                );
                It = Rc.GetResultEnumerator();

                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                await grainIntegerCompoundKey.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grainIntegerCompoundKey.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();

                // GrainWithIntegerCompoundKey
                var grainIntegerKey = GrainFactory.GetGrain<IReactiveGrainIntegerKey>(random.Next());
                Rc = GrainFactory.StartReactiveComputation(() =>
                    grainIntegerKey.GetValue()
                );
                It = Rc.GetResultEnumerator();

                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                await grainIntegerKey.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grainIntegerKey.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();

                // GrainWithIntegerCompoundKey
                var grainStringKey = GrainFactory.GetGrain<IReactiveGrainStringKey>(random.Next().ToString());
                Rc = GrainFactory.StartReactiveComputation(() =>
                    grainStringKey.GetValue()
                );
                It = Rc.GetResultEnumerator();

                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                await grainStringKey.SetValue("bar");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar");

                await grainStringKey.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();
            }

            public async Task CacheDependencyInvalidation(int randomoffset)
            {
                var grain1 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset);
                var grain2 = GrainFactory.GetGrain<IMyOtherReactiveGrain>(randomoffset + 1);

                var Rc = GrainFactory.StartReactiveComputation(async () =>
                {
                    var res = await grain1.GetValue();
                    if (!res.Equals("foo"))
                    {
                        res = await grain2.GetValue();
                    }
                    return res;
                });
                var It = Rc.GetResultEnumerator();

                var result = await It.NextResultAsync();
                Assert.Equal(result, "foo");

                await grain2.SetValue("bar2");
                await grain1.SetValue("bar1");
                await Task.Delay(1000);
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar2");
                await grain2.SetValue("bar3");
                result = await It.NextResultAsync();
                Assert.Equal(result, "bar3");

                await grain1.SetValue("foo");
                result = await It.NextResultAsync();
                Assert.Equal(result, "foo");
                Rc.Dispose();
            }
        }
    }

}
