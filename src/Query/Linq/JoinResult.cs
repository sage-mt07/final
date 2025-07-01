using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Query.Linq;

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
