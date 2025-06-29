using Kafka.Ksql.Linq.Query.Linq;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class JoinDslUnitTests
{
    private class DummyContext : IKsqlContext
    {
        public IEntitySet<T> Set<T>() where T : class => new StubSet<T>(this);
        public object GetEventSet(Type entityType) => throw new NotImplementedException();
        public Dictionary<Type, EntityModel> GetEntityModels() => new();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private class StubSet<T> : IEntitySet<T> where T : class
    {
        public readonly List<T> Added = new();
        public readonly List<T> Items = new();
        private readonly IKsqlContext _context;
        private readonly EntityModel _model;

        public StubSet(IKsqlContext context)
        {
            _context = context;
            _model = new EntityModel
            {
                EntityType = typeof(T),
                TopicAttribute = new TopicAttribute("topic"),
                AllProperties = typeof(T).GetProperties(),
                KeyProperties = Array.Empty<PropertyInfo>(),
                ValidationResult = new ValidationResult { IsValid = true }
            };
        }

        public Task AddAsync(T entity, CancellationToken cancellationToken = default)
        {
            Added.Add(entity);
            Items.Add(entity);
            return Task.CompletedTask;
        }

        public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new List<T>(Items));

        public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
            => Task.WhenAll(Items.Select(action));

        public string GetTopicName() => _model.TopicAttribute?.TopicName ?? typeof(T).Name;
        public EntityModel GetEntityModel() => _model;
        public IKsqlContext GetContext() => _context;

        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            foreach (var item in Items)
            {
                yield return item;
                await Task.Yield();
            }
        }
    }

    private class JoinProjection
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void ParameterReplacementVisitor_ReplacesMultipleParameters()
    {
        Expression<Func<int, int, bool>> expr = (a, b) => a > b;
        var replacements = new Dictionary<ParameterExpression, Expression>
        {
            [expr.Parameters[0]] = Expression.Constant(10),
            [expr.Parameters[1]] = Expression.Constant(5)
        };
        var visitor = new ParameterReplacementVisitor(replacements);
        var newBody = visitor.Visit(expr.Body);
        var lambda = Expression.Lambda<Func<bool>>(newBody);
        Assert.True(lambda.Compile()());
    }

    [Fact]
    public void ReplaceParameterVisitor_ReplacesParameter()
    {
        var p = Expression.Parameter(typeof(int), "p");
        var body = Expression.Add(p, Expression.Constant(1));
        var expr = Expression.Lambda<Func<int, int>>(body, p);
        var replaced = ReplaceParameterVisitor.Replace(expr.Body, p, Expression.Constant(5));
        var lambda = Expression.Lambda<Func<int>>(replaced);
        Assert.Equal(6, lambda.Compile()());
    }

    [Fact]
    public async Task EntitySetAdapter_ForwardsAddAndEnumeration()
    {
        var ctx = new DummyContext();
        var inner = new StubSet<TestEntity>(ctx);
        var adapter = new EntitySetAdapter<TestEntity>(inner);
        var entity = new TestEntity { Id = 1 };

        await adapter.AddAsync(entity);
        Assert.Contains(entity, inner.Added);
        var list = await adapter.ToListAsync();
        Assert.Single(list);
        Assert.Equal(entity, list[0]);

        var enumerated = new List<object>();
        await foreach (var item in adapter)
        {
            enumerated.Add(item);
        }
        Assert.Single(enumerated);
        Assert.Equal(entity, enumerated[0]);
    }

    [Fact]
    public void JoinableEntitySet_Join_ReturnsResult()
    {
        var ctx = new DummyContext();
        var outer = new JoinableEntitySet<TestEntity>(new StubSet<TestEntity>(ctx));
        var inner = new StubSet<ChildEntity>(ctx);
        var result = outer.Join(
            inner,
            (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
            (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId));
        Assert.NotNull(result);
    }

    [Fact]
    public async Task JoinResult_Select_ToListAsync()
    {
        var ctx = new DummyContext();
        var outer = new JoinableEntitySet<TestEntity>(new StubSet<TestEntity>(ctx));
        var inner = new StubSet<ChildEntity>(ctx);
        var join = outer.Join(
            inner,
            (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
            (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId));
        var result = join.Select((o, i) => new { o.Id, i.Name });
        var list = await result.ToListAsync();
        Assert.NotNull(list);
    }

    [Fact]
    public async Task ThreeWayJoinResult_Select_ToListAsync()
    {
        var ctx = new DummyContext();
        var outer = new JoinableEntitySet<TestEntity>(new StubSet<TestEntity>(ctx));
        var inner = new StubSet<ChildEntity>(ctx);
        var third = new StubSet<GrandChildEntity>(ctx);
        var join = outer.Join(
                            inner,
                            (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
                            (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId))
                        .Join(
                            third,
                            (Expression<Func<ChildEntity, object>>)(i => (object)i.Id),
                            (Expression<Func<GrandChildEntity, object>>)(t => (object)t.ChildId));
        var result = join.Select((o, i, t) => new { o.Id, t.Description });
        var list = await result.ToListAsync();
        Assert.NotNull(list);
    }

    [Fact]
    public async Task TypedJoinResultEntitySet_AddAsync_Throws()
    {
        var ctx = new DummyContext();
        var outer = new JoinableEntitySet<TestEntity>(new StubSet<TestEntity>(ctx));
        var inner = new StubSet<ChildEntity>(ctx);
        var result = outer.Join(
                          inner,
                          (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
                          (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId))
                          .Select((o, i) => new { o.Id, i.Name });
        await Assert.ThrowsAsync<NotSupportedException>(() => result.AddAsync(new { Id = 1, Name = "a" }));
    }

    [Fact]
    public async Task TypedThreeWayJoinResultEntitySet_AddAsync_Throws()
    {
        var ctx = new DummyContext();
        var outer = new JoinableEntitySet<TestEntity>(new StubSet<TestEntity>(ctx));
        var inner = new StubSet<ChildEntity>(ctx);
        var third = new StubSet<GrandChildEntity>(ctx);
        var result = outer.Join(
                          inner,
                          (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
                          (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId))
                          .Join(
                                third,
                                (Expression<Func<ChildEntity, object>>)(i => (object)i.Id),
                                (Expression<Func<GrandChildEntity, object>>)(t => (object)t.ChildId))
                          .Select((o, i, t) => new { o.Id, t.Description });
        await Assert.ThrowsAsync<NotSupportedException>(() => result.AddAsync(new { Id = 1, Description = "a" }));
    }

    [Fact]
    public void JoinResultEntitySet_ToString_ReturnsSql()
    {
        var ctx = new DummyContext();
        var builder = new JoinBuilder();

        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();

        var expr = outer.Join(inner, o => o.Id, i => i.ParentId, (o, i) => new JoinProjection { Id = o.Id, Name = i.Name }).Expression;

        var model = new EntityModel
        {
            EntityType = typeof(JoinProjection),
            TopicAttribute = new TopicAttribute("join_projection"),
            AllProperties = typeof(JoinProjection).GetProperties(),
            KeyProperties = Array.Empty<PropertyInfo>(),
            ValidationResult = new ValidationResult { IsValid = true }
        };

        var entitySet = new JoinResultEntitySet<JoinProjection>(ctx, model, expr, builder);

        var str = entitySet.ToString();

        Assert.Contains("JOIN", str);
    }

    [Fact]
    public void UnifiedJoinResult_Join_ReturnsResult()
    {
        var ctx = new DummyContext();
        var builder = new JoinBuilder();
        var outer = new StubSet<TestEntity>(ctx);
        var inner = new StubSet<ChildEntity>(ctx);
        var unified = new UnifiedJoinResult<TestEntity, ChildEntity>(
            outer,
            inner,
            (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
            (Expression<Func<ChildEntity, object>>)(i => (object)i.ParentId),
            builder,
            ctx);
        var threeWay = unified.Join(
            new StubSet<GrandChildEntity>(ctx),
            (Expression<Func<TestEntity, object>>)(o => (object)o.Id),
            (Expression<Func<GrandChildEntity, object>>)(g => (object)g.ChildId));

        Assert.NotNull(threeWay);
    }
}
