using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Linq;

internal class UnifiedJoinResult<TOuter, TInner> : IJoinResult<TOuter, TInner>
      where TOuter : class
      where TInner : class
{
    private readonly IEntitySet<TOuter> _outer;
    private readonly IEntitySet<TInner> _inner;
    private readonly Expression _outerKeySelector;
    private readonly Expression _innerKeySelector;
    private readonly JoinBuilder _joinBuilder;
    private readonly IKsqlContext _context;

    public UnifiedJoinResult(
        IEntitySet<TOuter> outer,
        IEntitySet<TInner> inner,
        Expression outerKeySelector,
        Expression innerKeySelector,
        JoinBuilder joinBuilder,
        IKsqlContext context)
    {
        _outer = outer ?? throw new ArgumentNullException(nameof(outer));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _outerKeySelector = outerKeySelector ?? throw new ArgumentNullException(nameof(outerKeySelector));
        _innerKeySelector = innerKeySelector ?? throw new ArgumentNullException(nameof(innerKeySelector));
        _joinBuilder = joinBuilder ?? throw new ArgumentNullException(nameof(joinBuilder));
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public IEntitySet<TResult> Select<TResult>(
        Expression<Func<TOuter, TInner, TResult>> resultSelector) where TResult : class
    {
        var joinExpression = BuildTwoTableJoinExpression(resultSelector);
        var resultEntityModel = CreateResultEntityModel<TResult>();

        return new JoinResultEntitySet<TResult>(
            _context,
            resultEntityModel,
            joinExpression,
            _joinBuilder);
    }

    public IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(
        IEntitySet<TThird> third,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class
    {
        return new UnifiedThreeWayJoinResult<TOuter, TInner, TThird>(
            _outer,
            _inner,
            third,
            _outerKeySelector,
            _innerKeySelector,
            outerKeySelector,
            thirdKeySelector,
            JoinKeySource.Outer,
            _joinBuilder,
            _context);
    }

    public IJoinResult<TOuter, TInner, TThird> Join<TThird, TKey>(
        IEntitySet<TThird> third,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TThird, TKey>> thirdKeySelector) where TThird : class
    {
        return new UnifiedThreeWayJoinResult<TOuter, TInner, TThird>(
            _outer,
            _inner,
            third,
            _outerKeySelector,
            _innerKeySelector,
            innerKeySelector,
            thirdKeySelector,
            JoinKeySource.Inner,
            _joinBuilder,
            _context);
    }

    private Expression BuildTwoTableJoinExpression<TResult>(
        Expression<Func<TOuter, TInner, TResult>> resultSelector)
    {
        var joinCallExpression = Expression.Call(
            typeof(System.Linq.Queryable),
            "Join",
            new[] { typeof(TOuter), typeof(TInner), typeof(object), typeof(TResult) },
            Expression.Constant(_outer),
            Expression.Constant(_inner),
            _outerKeySelector,
            _innerKeySelector,
            resultSelector);

        return joinCallExpression;
    }

    private EntityModel CreateResultEntityModel<TResult>() where TResult : class
    {
        return new EntityModel
        {
            EntityType = typeof(TResult),
            TopicAttribute = null,
            AllProperties = typeof(TResult).GetProperties(),
            KeyProperties = Array.Empty<System.Reflection.PropertyInfo>()
        };
    }
}
