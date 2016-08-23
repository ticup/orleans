using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Providers;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{

    public class ReactiveGrainBase : Grain
    {
        string Value = "foo";

        public Task<string> GetValue(int offset = 0)
        {
            return Task.FromResult(Value);
        }

        public Task SetValue(string value)
        {
            Value = value;
            return TaskDone.Done;
        }
    }

    public class MyReactiveGrain : Grain, IMyReactiveGrain
    {
        List<IMyOtherReactiveGrain> Grains = new List<IMyOtherReactiveGrain>();
        string MyString = "foo";

        public Task<string> MyComp(string someArg)
        {
            return Task.FromResult(MyString);
        }

        public Task SetString(string newString)
        {
            MyString = newString;
            return TaskDone.Done;
        }



        public Task SetGrains(List<IMyOtherReactiveGrain> grains) {
            Grains = grains;
            return TaskDone.Done;
        }

        public async Task<string> MyLayeredComputation()
        {
            var Tasks = this.Grains.Select((g) => g.GetValue());
            var Strings = await Task.WhenAll(Tasks);
            return string.Join(" ", Strings);
        }
       

    }


    public class MyOtherReactiveGrain : Grain, IMyOtherReactiveGrain
    {
        string MyString = "foo";


        public Task<string> GetValue(int offset = 0)
        {
            return Task.FromResult(MyString);
        }

        public Task SetValue(string newValue)
        {
            MyString = newValue;
            return TaskDone.Done;
        }

        public Task<bool> FaultyMethod()
        {
            if (MyString == "fault")
            {
                throw new Exception("faulted");
            }
            return Task.FromResult(true);
        }
    }


    public class ReactiveGrainGuidCompoundKey : ReactiveGrainBase, IReactiveGrainGuidCompoundKey { }
    public class ReactiveGrainGuidKey : ReactiveGrainBase, IReactiveGrainGuidKey { }
    public class ReactiveGrainIntegerCompoundKey : ReactiveGrainBase, IReactiveGrainIntegerCompoundKey { }
    public class ReactiveGrainIntegerKey : ReactiveGrainBase, IReactiveGrainIntegerKey { }
    public class ReactiveGrainStringKey : ReactiveGrainBase, IReactiveGrainStringKey { }


    public class UserGrainState
    {
        public HashSet<string> Subscriptions { get; set; }
        public int ChunkNumber { get; set; }

    }


    public class ChirperUserGrain : Grain, IChirperUserGrain
    {
        HashSet<string> Subscriptions;
        int ChunkNumber;
        IMessageChunkGrain CurrentChunk;
        public string Name { get; set; }


        /// <summary>
        /// Add given user to the subscription list
        /// </summary>
        public Task Follow(string userName)
        {
            Subscriptions.Add(userName);
            //return WriteStateAsync();
            return TaskDone.Done;
        }


        /// <summary>
        /// Remove given user from the subscription list
        /// </summary>
        public Task Unfollow(string userName)
        {
            Subscriptions.Remove(userName);
            //return WriteStateAsync();
            return TaskDone.Done;
        }


        /// <summary>
        /// Get the Users this user is subscribed to
        /// </summary>
        public Task<List<string>> GetFollowersList()
        {
            return Task.FromResult(Subscriptions.ToList());
        }


        /// <summary>
        ///  Get range amount of messages posted by this user
        /// </summary>
        public async Task<List<UserMessage>> GetMessages(int range)
        {
            var NoChunks = Math.Ceiling((double)range / MessageChunkGrain.MessageChunkSize);
            var Tasks = new List<Task<List<UserMessage>>>();
            for (int i = 0; i <= ChunkNumber; i++)
            {
                if (i > NoChunks) break;
                var chunkGrain = GrainFactory.GetGrain<IMessageChunkGrain>(Name + "." + i);
                Tasks.Add(chunkGrain.getMessages());
            }
            List<UserMessage> Msgs = (await Task.WhenAll(Tasks)).SelectMany(x => x).ToList();
            return Msgs;
        }


        /// <summary>
        /// Post given text as a message
        /// </summary>
        public async Task<bool> PostText(string text)
        {
            //  add the message to the current chunk
            var msg = new UserMessage(text, Name);
            bool succeed = await CurrentChunk.AddMessage(msg);
            if (succeed) return true;

            // chunk was full, add new one and try again (only once, not recursive).
            await AddChunk();
            return await CurrentChunk.AddMessage(msg);
        }


        /// <summary>
        /// Get messages from all the user its subscriptions,
        /// combine them with its own messages,
        /// sort by timestamp and return the first limit number.
        /// </summary>
        public async Task<Timeline> GetTimeline(int limit)
        {
            // i) request a number of messages from each subscription
            var Tasks = Subscriptions.Select(UserName =>
            {
                IChirperUserGrain user = GrainFactory.GetGrain<IChirperUserGrain>(UserName);
                return user.GetMessages(limit);
            }).ToList();

            // ii) request your own messages
            Tasks.Add(GetMessages(limit));

            // iii) await for all the messages and flatten them
            List<UserMessage> Msgs = (await Task.WhenAll(Tasks)).SelectMany((x) => x).ToList();

            // iv) order by Timestamp and take the first <limit> messages
            return new Timeline(Msgs.OrderBy((m) => m.Timestamp).Take(limit).ToList());
        }


        /// <summary>
        /// "Create" a new chunk, by increment the ChunkNumber
        /// </summary>
        /// <returns></returns>
        private Task AddChunk()
        {
            ChunkNumber++;
            SetCurrentChunk();
            //return WriteStateAsync();
            return TaskDone.Done;
        }


        /// <summary>
        ///  Sets the CurrentChunk to the grain instance that belongs to the current ChunkNumber
        /// </summary>
        private void SetCurrentChunk()
        {
            CurrentChunk = GrainFactory.GetGrain<IMessageChunkGrain>(Name + "." + ChunkNumber);
        }


        public override Task OnActivateAsync()
        {
            // First time activating this grain instance
            if (Subscriptions == null)
            {
                Subscriptions = new HashSet<string>();
            }

            Name = this.GetPrimaryKeyString();
            SetCurrentChunk();
            return TaskDone.Done;
        }


    }

    class MessageChunkGrainState
    {
        public List<ChirperUserGrain> Msgs { get; set; }
    }

    //[StorageProvider(ProviderName = "MemoryStore")]
    class MessageChunkGrain : Grain, IMessageChunkGrain
    {
        public static double MessageChunkSize = 10;
        List<UserMessage> Msgs;



        public Task<List<UserMessage>> getMessages()
        {
            return Task.FromResult(Msgs);
        }


        /// <summary>
        /// Add a message to the MessageList.
        /// </summary>
        /// <param name="message"></param>
        /// <returns>
        /// False if the message could not be added because the list was full,
        /// True otherwise.
        /// </returns>
        public Task<bool> AddMessage(UserMessage message)
        {
            if (Msgs.Count > MessageChunkSize)
            {
                return Task.FromResult(false);
            }
            Msgs.Add(message);
            //await WriteStateAsync();
            return Task.FromResult(true);
        }



        public override Task OnActivateAsync()
        {
            if (Msgs == null)
            {
                Msgs = new List<UserMessage>();
            }
            return TaskDone.Done;
        }
    }

}
