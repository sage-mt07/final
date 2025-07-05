using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Context;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Extensions;

/// <summary>
/// LINQクエリチェーンの利用制限拡張。
/// Where/GroupBy/SelectなどはOnModelCreating専用。
/// </summary>
public static class QueryRestrictionExtensions
{
    private static void EnsureOnModelCreating<T>(IEntitySet<T> set) where T : class
    {
        var ctx = set.GetContext();
        if (ctx is KafkaContextCore core && !core.IsInModelCreating)
        {
            throw new InvalidOperationException("Where/GroupBy/Selectのクエリ定義はOnModelCreating専用です。");
        }
    }

    public static IEntitySet<T> Where<T>(this IEntitySet<T> source, Expression<Func<T, bool>> predicate) where T : class
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        EnsureOnModelCreating(source);
        return source;
    }

    public static IEntitySet<IGrouping<TKey, T>> GroupBy<TKey, T>(this IEntitySet<T> source, Expression<Func<T, TKey>> keySelector) where T : class
    {
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
        EnsureOnModelCreating(source);
        return new QueryDefinitionSet<IGrouping<TKey, T>>(source.GetContext());
    }

    public static IEntitySet<TResult> Select<T, TResult>(this IEntitySet<T> source, Expression<Func<T, TResult>> selector)
        where T : class where TResult : class
    {
        if (selector == null) throw new ArgumentNullException(nameof(selector));
        EnsureOnModelCreating(source);
        return new QueryDefinitionSet<TResult>(source.GetContext());
    }

    private class QueryDefinitionSet<T> : IEntitySet<T> where T : class
    {
        private readonly IKsqlContext _context;

        public QueryDefinitionSet(IKsqlContext context)
        {
            _context = context;
        }

        public Task AddAsync(T entity, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) => Task.FromResult(new List<T>());
        public Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public string GetTopicName() => typeof(T).Name;
        public EntityModel GetEntityModel() => new EntityModel { EntityType = typeof(T), TopicAttribute = new TopicAttribute(typeof(T).Name), AllProperties = typeof(T).GetProperties(), KeyProperties = Array.Empty<System.Reflection.PropertyInfo>() };
        public IKsqlContext GetContext() => _context;
        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) { await Task.CompletedTask; yield break; }
    }
}
