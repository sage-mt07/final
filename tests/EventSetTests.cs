using Kafka.Ksql.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests;

public class EventSetTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class TestSet : EventSet<TestEntity>
    {
        private readonly List<TestEntity> _items;
        public bool Sent { get; private set; }

        public TestSet(List<TestEntity> items, EntityModel model) : base(new DummyContext(), model)
        {
            _items = items;
        }

        protected override Task SendEntityAsync(TestEntity entity, CancellationToken cancellationToken)
        {
            Sent = true;
            return Task.CompletedTask;
        }

        public override async IAsyncEnumerator<TestEntity> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var item in _items)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                yield return item;
                await Task.Yield();
            }
        }
    }

    private static EntityModel CreateModel()
    {
        return new EntityModel
        {
            EntityType = typeof(TestEntity),
            TopicAttribute = new TopicAttribute("test-topic"),
            KeyProperties = new[] { typeof(TestEntity).GetProperty(nameof(TestEntity.Id))! },
            AllProperties = typeof(TestEntity).GetProperties()
        };
    }

    [Fact]
    public async Task AddAsync_NullEntity_Throws()
    {
        var set = new TestSet(new(), CreateModel());
        await Assert.ThrowsAsync<ArgumentNullException>(() => set.AddAsync(null!));
    }

    [Fact]
    public async Task ToListAsync_ReturnsItems()
    {
        var items = new List<TestEntity> { new TestEntity { Id = 1 } };
        var set = new TestSet(items, CreateModel());
        var result = await set.ToListAsync();
        Assert.Single(result);
        Assert.Equal(1, result[0].Id);
    }

    [Fact]
    public async Task ForEachAsync_InvokesAction()
    {
        var items = new List<TestEntity> { new TestEntity { Id = 1 }, new TestEntity { Id = 2 } };
        var set = new TestSet(items, CreateModel());
        var sum = 0;
        await set.ForEachAsync(e => { sum += e.Id; return Task.CompletedTask; });
        Assert.Equal(3, sum);
    }


    [Fact]
    public void Metadata_ReturnsExpectedValues()
    {
        var model = CreateModel();
        var set = new TestSet(new(), model);
        Assert.Equal("test-topic", set.GetTopicName());
        Assert.Equal(model, set.GetEntityModel());
        Assert.IsType<DummyContext>(set.GetContext());
        var str = set.ToString();
        Assert.Contains("EventSet<TestEntity>", str);
        Assert.Contains("test-topic", str);
    }
}
