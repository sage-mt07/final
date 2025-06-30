using Kafka.Ksql.Linq.Query.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class TypedJoinResultEntitySetTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => throw new NotImplementedException();
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class DummySet<T> : IEntitySet<T> where T : class
    {
        private readonly IKsqlContext _context = new DummyContext();
        public Task AddAsync(T entity, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(new List<T>());
        public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public string GetTopicName() => typeof(T).Name;
        public EntityModel GetEntityModel() => new EntityModel { EntityType = typeof(T), TopicAttribute = new TopicAttribute(typeof(T).Name), AllProperties = typeof(T).GetProperties(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>(), ValidationResult = new ValidationResult(true, new()) };
        public IKsqlContext GetContext() => _context;
        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) { await Task.CompletedTask; yield break; }
    }

    private record Result(int Id, string Name);

    [Fact]
    public async Task ToListAsync_ReturnsEmptyList()
    {
        var ctx = new DummyContext();
        var outer = new DummySet<TestEntity>();
        var inner = new DummySet<ChildEntity>();
        var model = new EntityModel { EntityType = typeof(Result), TopicAttribute = new TopicAttribute("R"), AllProperties = typeof(Result).GetProperties(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>(), ValidationResult = new ValidationResult(true, new()) };

        Expression<Func<TestEntity, ChildEntity, Result>> selector = (o, i) => new Result(o.Id, i.Name);
        var set = new TypedJoinResultEntitySet<TestEntity, ChildEntity, Result>(ctx, model, outer, inner, o => o.Id, i => i.ParentId, selector);
        var list = await set.ToListAsync();
        Assert.Empty(list);
        Assert.Equal("R", set.GetTopicName());
    }

    [Fact]
    public void UnsupportedOperations_Throw()
    {
        var ctx = new DummyContext();
        var outer = new DummySet<TestEntity>();
        var inner = new DummySet<ChildEntity>();
        var model = new EntityModel { EntityType = typeof(Result), TopicAttribute = new TopicAttribute("R"), AllProperties = typeof(Result).GetProperties(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>(), ValidationResult = new ValidationResult(true, new()) };
        Expression<Func<TestEntity, ChildEntity, Result>> selector = (o, i) => new Result(o.Id, i.Name);
        var set = new TypedJoinResultEntitySet<TestEntity, ChildEntity, Result>(ctx, model, outer, inner, o => o.Id, i => i.ParentId, selector);
        Assert.Throws<NotSupportedException>(() => set.AddAsync(new Result(1, "a")));
        Assert.Throws<NotSupportedException>(() => set.ForEachAsync(_ => Task.CompletedTask));
    }
}

public class TestEntity { public int Id { get; set; } }
public class ChildEntity { public int ParentId { get; set; } public string Name { get; set; } = string.Empty; }
