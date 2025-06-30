using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Linq;

/// <summary>
/// JOIN操作対応EntitySet（document index 21の補完版）
/// 設計理由：既存実装の不足部分を補完し、新アーキテクチャとの統合を実現
/// </summary>
public class JoinableEntitySet<T> : IEntitySet<T>, IJoinableEntitySet<T> where T : class
{
    private readonly IEntitySet<T> _baseEntitySet;

    public JoinableEntitySet(IEntitySet<T> baseEntitySet)
    {
        _baseEntitySet = baseEntitySet ?? throw new ArgumentNullException(nameof(baseEntitySet));
    }

    // ✅ IEntitySet<T>の必須実装
    public async Task AddAsync(T entity, CancellationToken cancellationToken = default)
    {
        await _baseEntitySet.AddAsync(entity, cancellationToken);
    }

    public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return await _baseEntitySet.ToListAsync(cancellationToken);
    }

    public async Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        await _baseEntitySet.ForEachAsync(action, timeout, cancellationToken);
    }

    public string GetTopicName() => _baseEntitySet.GetTopicName();

    public EntityModel GetEntityModel() => _baseEntitySet.GetEntityModel();

    public IKsqlContext GetContext() => _baseEntitySet.GetContext();

    // ✅ IAsyncEnumerable<T>の実装
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach (var item in _baseEntitySet.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    // ✅ IJoinableEntitySet<T>のJOIN機能実装
    public IJoinResult<T, TInner> Join<TInner, TKey>(
        IEntitySet<TInner> inner,
        Expression<Func<T, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class
    {
        if (inner == null)
            throw new ArgumentNullException(nameof(inner));
        if (outerKeySelector == null)
            throw new ArgumentNullException(nameof(outerKeySelector));
        if (innerKeySelector == null)
            throw new ArgumentNullException(nameof(innerKeySelector));

        return new JoinResult<T, TInner>(this, inner, outerKeySelector, innerKeySelector);
    }

    public override string ToString()
    {
        return $"JoinableEntitySet<{typeof(T).Name}> wrapping {_baseEntitySet}";
    }
}

/// <summary>
/// JOIN可能EntitySetインターフェース（補完版）
/// </summary>
public interface IJoinableEntitySet<T> where T : class
{
    IJoinResult<T, TInner> Join<TInner, TKey>(
        IEntitySet<TInner> inner,
        Expression<Func<T, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector) where TInner : class;
}

/// <summary>
/// JOIN結果インターフェース（補完版）
/// </summary>
public interface IJoinResult<TOuter, TInner>
    where TOuter : class
    where TInner : class
{
    IEntitySet<TResult> Select<TResult>(Expression<Func<TOuter, TInner, TResult>> resultSelector) where TResult : class;
    IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(IEntitySet<TThird> third, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class;
    IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(IEntitySet<TThird> third, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class;
}

/// <summary>
/// 3-way JOIN結果インターフェース（補完版）
/// </summary>
public interface IJoinResult<TOuter, TInner, TThird>
    where TOuter : class
    where TInner : class
    where TThird : class
{
    IEntitySet<TResult> Select<TResult>(Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector) where TResult : class;
}

/// <summary>
/// JOIN結果実装（document index 21からの移植・改良版）
/// </summary>
internal class JoinResult<TOuter, TInner> : IJoinResult<TOuter, TInner>
    where TOuter : class
    where TInner : class
{
    private readonly IEntitySet<TOuter> _outer;
    private readonly IEntitySet<TInner> _inner;
    private readonly Expression<Func<TOuter, object>> _outerKeySelector;
    private readonly Expression<Func<TInner, object>> _innerKeySelector;

    public JoinResult(
        IEntitySet<TOuter> outer,
        IEntitySet<TInner> inner,
        Expression outerKeySelector,
        Expression innerKeySelector)
    {
        _outer = outer ?? throw new ArgumentNullException(nameof(outer));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _outerKeySelector = (Expression<Func<TOuter, object>>)outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = (Expression<Func<TInner, object>>)innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
    }

    public IEntitySet<TResult> Select<TResult>(
        Expression<Func<TOuter, TInner, TResult>> resultSelector) where TResult : class
    {
        if (resultSelector == null)
            throw new ArgumentNullException(nameof(resultSelector));

        return new TypedJoinResultEntitySet<TOuter, TInner, TResult>(
               _outer.GetContext(),
               CreateResultEntityModel<TResult>(),
               _outer,
               _inner,
               _outerKeySelector,
               _innerKeySelector,
               resultSelector);
    }

    public IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(
        IEntitySet<TThird> third,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class
    {
        if (third == null)
            throw new ArgumentNullException(nameof(third));
        if (outerKeySelector == null)
            throw new ArgumentNullException(nameof(outerKeySelector));
        if (thirdKeySelector == null)
            throw new ArgumentNullException(nameof(thirdKeySelector));

        return new ThreeWayJoinResult<TOuter, TInner, TThird>(
            _outer, _inner, third,
            _outerKeySelector, _innerKeySelector,
            outerKeySelector, thirdKeySelector);
    }

    public IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(
        IEntitySet<TThird> third,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class
    {
        if (third == null)
            throw new ArgumentNullException(nameof(third));
        if (innerKeySelector == null)
            throw new ArgumentNullException(nameof(innerKeySelector));
        if (thirdKeySelector == null)
            throw new ArgumentNullException(nameof(thirdKeySelector));

        return new ThreeWayJoinResult<TOuter, TInner, TThird>(
            _outer, _inner, third,
            _outerKeySelector, _innerKeySelector,
            innerKeySelector, thirdKeySelector);
    }

    private static EntityModel CreateResultEntityModel<TResult>() where TResult : class
    {
        return new EntityModel
        {
            EntityType = typeof(TResult),
            TopicAttribute = new TopicAttribute($"{typeof(TResult).Name}_JoinResult"),
            AllProperties = typeof(TResult).GetProperties(),
            KeyProperties = Array.Empty<System.Reflection.PropertyInfo>(),
            ValidationResult = new ValidationResult { IsValid = true }
        };
    }
}

/// <summary>
/// 3-way JOIN結果実装（document index 21からの移植・改良版）
/// </summary>
internal class ThreeWayJoinResult<TOuter, TInner, TThird> : IJoinResult<TOuter, TInner, TThird>
    where TOuter : class
    where TInner : class
    where TThird : class
{
    private readonly IEntitySet<TOuter> _outer;
    private readonly IEntitySet<TInner> _inner;
    private readonly IEntitySet<TThird> _third;
    private readonly Expression _outerKeySelector;
    private readonly Expression _innerKeySelector;
    private readonly Expression _firstThirdKeySelector;
    private readonly Expression _secondThirdKeySelector;

    public ThreeWayJoinResult(
        IEntitySet<TOuter> outer,
        IEntitySet<TInner> inner,
        IEntitySet<TThird> third,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression firstThirdKeySelector,
        Expression secondThirdKeySelector)
    {
        _outer = outer ?? throw new ArgumentNullException(nameof(outer));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _third = third ?? throw new ArgumentNullException(nameof(third));
        _outerKeySelector = outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
        _firstThirdKeySelector = firstThirdKeySelector ?? throw new ArgumentNullException(nameof(firstThirdKeySelector));
        _secondThirdKeySelector = secondThirdKeySelector ?? throw new ArgumentNullException(nameof(secondThirdKeySelector));
    }

    public IEntitySet<TResult> Select<TResult>(
        Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector) where TResult : class
    {
        if (resultSelector == null)
            throw new ArgumentNullException(nameof(resultSelector));

        return new TypedThreeWayJoinResultEntitySet<TOuter, TInner, TThird, TResult>(
                      _outer.GetContext(),
                      CreateResultEntityModel<TResult>(),
                      _outer,
                      _inner,
                      _third,
                      _outerKeySelector,
                      _innerKeySelector,
                      _firstThirdKeySelector,
                      _secondThirdKeySelector,
                      resultSelector);
    }

    private static EntityModel CreateResultEntityModel<TResult>() where TResult : class
    {
        return new EntityModel
        {
            EntityType = typeof(TResult),
            TopicAttribute = new TopicAttribute($"{typeof(TResult).Name}_ThreeWayJoinResult"),
            AllProperties = typeof(TResult).GetProperties(),
            KeyProperties = Array.Empty<System.Reflection.PropertyInfo>(),
            ValidationResult = new ValidationResult { IsValid = true }
        };
    }
}

/// <summary>
/// 型安全なJOIN結果EntitySet（document index 21からの移植・改良版）
/// </summary>
internal class TypedJoinResultEntitySet<TOuter, TInner, TResult> : IEntitySet<TResult>
       where TOuter : class
       where TInner : class
       where TResult : class
{
    private readonly IKsqlContext _context;
    private readonly EntityModel _entityModel;
    private readonly IEntitySet<TOuter> _outerEntitySet;
    private readonly IEntitySet<TInner> _innerEntitySet;
    private readonly Expression<Func<TOuter, object>> _outerKeySelector;
    private readonly Expression<Func<TInner, object>> _innerKeySelector;
    private readonly Expression<Func<TOuter, TInner, TResult>> _resultSelector;

    public TypedJoinResultEntitySet(
        IKsqlContext context,
        EntityModel entityModel,
        IEntitySet<TOuter> outerEntitySet,
        IEntitySet<TInner> innerEntitySet,
        Expression<Func<TOuter, object>> outerKeySelector,
        Expression<Func<TInner, object>> innerKeySelector,
        Expression<Func<TOuter, TInner, TResult>> resultSelector)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
        _outerEntitySet = outerEntitySet ?? throw new ArgumentNullException(nameof(outerEntitySet));
        _innerEntitySet = innerEntitySet ?? throw new ArgumentNullException(nameof(innerEntitySet));
        _outerKeySelector = outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
        _resultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
    }

    public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // JOIN処理の実装（簡略版）
        await Task.Delay(100, cancellationToken);
        return new List<TResult>();
    }

    public Task AddAsync(TResult entity, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Cannot add entities to a join result set");
    }

    public Task ForEachAsync(Func<TResult, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("ForEachAsync not supported on join result sets");
    }

    public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? typeof(TResult).Name;
    public EntityModel GetEntityModel() => _entityModel;
    public IKsqlContext GetContext() => _context;

    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var item in results)
        {
            yield return item;
        }
    }
}

/// <summary>
/// 型安全な3-way JOIN結果EntitySet（document index 21からの移植・改良版）
/// </summary>
internal class TypedThreeWayJoinResultEntitySet<TOuter, TInner, TThird, TResult> : IEntitySet<TResult>
    where TOuter : class
    where TInner : class
    where TThird : class
    where TResult : class
{
    private readonly IKsqlContext _context;
    private readonly EntityModel _entityModel;
    private readonly IEntitySet<TOuter> _outerEntitySet;
    private readonly IEntitySet<TInner> _innerEntitySet;
    private readonly IEntitySet<TThird> _thirdEntitySet;
    private readonly Expression _outerKeySelector;
    private readonly Expression _innerKeySelector;
    private readonly Expression _firstThirdKeySelector;
    private readonly Expression _secondThirdKeySelector;
    private readonly Expression<Func<TOuter, TInner, TThird, TResult>> _resultSelector;

    public TypedThreeWayJoinResultEntitySet(
        IKsqlContext context,
        EntityModel entityModel,
        IEntitySet<TOuter> outerEntitySet,
        IEntitySet<TInner> innerEntitySet,
        IEntitySet<TThird> thirdEntitySet,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression firstThirdKeySelector,
        Expression secondThirdKeySelector,
        Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _entityModel = entityModel ?? throw new ArgumentNullException(nameof(entityModel));
        _outerEntitySet = outerEntitySet ?? throw new ArgumentNullException(nameof(outerEntitySet));
        _innerEntitySet = innerEntitySet ?? throw new ArgumentNullException(nameof(innerEntitySet));
        _thirdEntitySet = thirdEntitySet ?? throw new ArgumentNullException(nameof(thirdEntitySet));
        _outerKeySelector = outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
        _firstThirdKeySelector = firstThirdKeySelector ?? throw new ArgumentNullException(nameof(firstThirdKeySelector));
        _secondThirdKeySelector = secondThirdKeySelector ?? throw new ArgumentNullException(nameof(secondThirdKeySelector));
        _resultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
    }

    public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // 3-way JOIN処理の実装（簡略版）
        await Task.Delay(100, cancellationToken);
        return new List<TResult>();
    }

    public Task AddAsync(TResult entity, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Cannot add entities to a three-way join result set");
    }

    public Task ForEachAsync(Func<TResult, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("ForEachAsync not supported on three-way join result sets");
    }

    public string GetTopicName() => _entityModel.TopicAttribute?.TopicName ?? typeof(TResult).Name;
    public EntityModel GetEntityModel() => _entityModel;
    public IKsqlContext GetContext() => _context;

    public async IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken);
        foreach (var item in results)
        {
            yield return item;
        }
    }
}
