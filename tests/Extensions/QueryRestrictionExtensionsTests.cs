using Kafka.Ksql.Linq.Application;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Extensions;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Extensions;

public class QueryRestrictionExtensionsTests
{
    private class Sample { public int Id { get; set; } }

    private class ValidContext : KsqlContext
    {
        public ValidContext() : base() { }
        protected override bool SkipSchemaRegistration => true;
        protected override void OnModelCreating(IModelBuilder modelBuilder)
        {
            var set = Set<Sample>();
            set.Where(e => e.Id > 0);
        }
        protected override IEntitySet<T> CreateEntitySet<T>(EntityModel model)
        {
            return new StubSet<T>(this, model);
        }
    }

    private class StubSet<T> : IEntitySet<T> where T : class
    {
        private readonly IKsqlContext _context;
        private readonly EntityModel _model;
        public StubSet(IKsqlContext context, EntityModel model)
        {
            _context = context; _model = model;
        }
        public Task AddAsync(T entity, System.Threading.CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<System.Collections.Generic.List<T>> ToListAsync(System.Threading.CancellationToken cancellationToken = default) => Task.FromResult(new System.Collections.Generic.List<T>());
        public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, System.Threading.CancellationToken cancellationToken = default) => Task.CompletedTask;
        public string GetTopicName() => _model.TopicAttribute?.TopicName ?? typeof(T).Name;
        public EntityModel GetEntityModel() => _model;
        public IKsqlContext GetContext() => _context;
        public async IAsyncEnumerator<T> GetAsyncEnumerator(System.Threading.CancellationToken cancellationToken = default) { await Task.CompletedTask; yield break; }
    }

    [Fact]
    public void Where_OutsideModelCreating_Throws()
    {
        var ctx = new ValidContext();
        var set = ctx.Set<Sample>();
        Assert.Throws<InvalidOperationException>(() => set.Where(s => s.Id > 0));
    }

    [Fact]
    public void ContextCreation_WithQueryChain_DoesNotThrow()
    {
        new ValidContext();
    }
}
