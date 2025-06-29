using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Linq;

internal class JoinResultEntitySet<T> : IEntitySet<T> where T : class
{
    private readonly IKsqlContext _context;
    private readonly EntityModel _entityModel;
    private readonly Expression _joinExpression;
    private readonly JoinBuilder _joinBuilder;

    public JoinResultEntitySet(
        IKsqlContext context,
        EntityModel entityModel,
        Expression joinExpression,
        JoinBuilder joinBuilder)
    {
        _context = context ?? throw new System.ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new System.ArgumentNullException(nameof(entityModel));
        _joinExpression = joinExpression;
        _joinBuilder = joinBuilder;
    }

    // ✅ IEntitySet<T>の実装 - ExecuteQueryAsyncメソッドは削除（基底クラスに存在しない）
    public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // JoinBuilderを使用してKSQL文を生成
        var ksqlQuery = _joinBuilder.Build(_joinExpression);

        // 実際のクエリ実行は具象実装に委譲
        // TODO: KsqlDbExecutorを使用してクエリ実行
        return await Task.FromResult(new List<T>());
    }

    public Task AddAsync(T entity, CancellationToken cancellationToken = default)
    {
        throw new System.NotSupportedException("Cannot add entities to a join result set");
    }

    public async Task ForEachAsync(System.Func<T, Task> action, System.TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var result in results)
        {
            await action(result);
        }
    }

    public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? typeof(T).Name;

    public EntityModel GetEntityModel() => _entityModel;

    public IKsqlContext GetContext() => _context;

    // ✅ IAsyncEnumerable<T>の実装
    public async System.Collections.Generic.IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var result in results)
        {
            yield return result;
        }
    }

    public override string ToString()
    {
        return $"JoinResult<{typeof(T).Name}> - Generated KSQL: {_joinBuilder.Build(_joinExpression)}";
    }
}
