using Orleans;
using Orleans.Reactive;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public interface IMyReactiveGrain : IGrainWithIntegerKey
    {
        Task<string> MyLayeredComputation();
        Task SetString(string newString);
        Task SetGrains(List<IMyOtherReactiveGrain> grains);
    }

    public interface IMyOtherReactiveGrain : IGrainWithIntegerKey
    {
        Task<string> GetValue(int offset = 0);

        Task SetValue(string newValue);
        Task<bool> FaultyMethod();

    }

    public interface IReactiveGrainBase
    {
        Task<string> GetValue(int offset = 0);
        Task SetValue(string newValue);
    }

    public interface IReactiveGrainGuidCompoundKey : IGrainWithGuidCompoundKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainGuidKey : IGrainWithGuidKey, IReactiveGrainBase
    { 
    }

    public interface IReactiveGrainIntegerCompoundKey : IGrainWithIntegerCompoundKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainIntegerKey : IGrainWithIntegerKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainStringKey : IGrainWithStringKey, IReactiveGrainBase
    {
    }

    public interface IReactiveGrainTestsGrain: IGrainWithIntegerKey
    {
        Task OnUpdateAsyncAfterUpdate(int randomoffset);
        Task OnUpdateAsyncBeforeUpdate(int randomoffset);
        Task OnUpdateAsyncBeforeUpdate2(int randomoffset);
        Task DontPropagateWhenNoChange(int randomoffset);
        Task FilterIdenticalResults(int randomoffset);
        Task MultipleIteratorsSameComputation(int randomoffsett);
        Task MultiLayeredComputation(int randomoffset);
        Task IteratorShouldOnlyReturnLatestValue(int randomoffset);
        Task MultipleComputationsUsingSameMethodSameActivation(int randomoffset);
        Task MultipleComputationsUsingSameMethodDifferentActivation(int randomoffset);
        Task MultipleCallsFromSameComputation(int randomoffset);
        Task ExceptionPropagation(int randomoffset);
        Task GrainKeyTypes(int randomoffset);
        Task CacheDependencyInvalidation(int randomoffset);

    }

    public interface IMessageChunkGrain : Orleans.IGrainWithStringKey
    {
        Task<List<UserMessage>> getMessages();

        Task<bool> AddMessage(UserMessage message);
    }

    public interface IChirperUserGrain : IGrainWithStringKey
    {
        Task<List<string>> GetFollowersList();
        Task<List<UserMessage>> GetMessages(int amount);
        Task<Timeline> GetTimeline(int amount);

        Task Follow(string userName);
        Task<bool> PostText(string text);
    }

    [Serializable]
    public class UserMessage
    {
        public Guid MessageId { get; private set; }
        public string Text { get; set; }
        public DateTime Timestamp { get; set; }
        public string Username { get; set; }


        public UserMessage(string text, string userName)
        {
            this.MessageId = Guid.NewGuid();
            this.Text = text;
            this.Username = userName;
            this.Timestamp = DateTime.Now;
        }

        public override string ToString()
        {
            StringBuilder str = new StringBuilder();
            str.Append("Message: '").Append(Text).Append("'");
            str.Append(" from @").Append(Username);
            return str.ToString();
        }
    }

    public class Timeline
    {
        public List<UserMessage> Posts { get; set; }
        public Timeline(List<UserMessage> posts)
        {
            Posts = posts;
        }
    }

}
