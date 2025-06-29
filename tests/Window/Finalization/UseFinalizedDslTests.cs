using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.StateStore.Extensions;
using Kafka.Ksql.Linq.StateStore.Management;
using Kafka.Ksql.Linq.StateStore;
using Xunit;
using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests.Window.Finalization;

public class UseFinalizedDslTests
{
    private class DummyContext : IKsqlContext
    {
        private class StubSet<T> : IEntitySet<T> where T : class
        {
            private readonly IKsqlContext _context;

            public StubSet(IKsqlContext context)
            {
                _context = context;
            }

            public Task AddAsync(T entity, CancellationToken cancellationToken = default) => Task.CompletedTask;
            public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(new List<T>());
            public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default) => Task.CompletedTask;
            public string GetTopicName() => typeof(T).Name;
            public EntityModel GetEntityModel() => new EntityModel { EntityType = typeof(T), TopicAttribute = new TopicAttribute("orders"), AllProperties = typeof(T).GetProperties(), KeyProperties = new[] { typeof(T).GetProperty("Id")! } };
            public IKsqlContext GetContext() => _context;
            public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) { await Task.CompletedTask; yield break; }
        }

        public IEntitySet<T> Set<T>() where T : class => new StubSet<T>(this);
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class Sample { public int Id { get; set; } }

    [Fact]
    public void UseFinalized_ReturnsSetWithFinalTopicName()
    {
        var ctx = new DummyContext();
        var options = new KsqlDslOptions();
        KafkaContextStateStoreExtensions.InitializeStateStores(ctx, options);
        var manager = ctx.GetStateStoreManager()!;

        var model = new EntityModel
        {
            EntityType = typeof(Sample),
            TopicAttribute = new TopicAttribute("orders"),
            AllProperties = typeof(Sample).GetProperties(),
            KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! }
        };

        var baseSet = new Kafka.Ksql.Linq.StateStore.WindowedEntitySet<Sample>(ctx.Set<Sample>(), 5, (StateStoreManager)manager, model);
        var finalSet = baseSet.UseFinalized();

        Assert.Equal("orders_window_5_final", finalSet.GetTopicName());
    }
}
