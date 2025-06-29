using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Linq;

internal class UnifiedThreeWayJoinResult<TOuter, TInner, TThird> : IJoinResult<TOuter, TInner, TThird>
        where TOuter : class
        where TInner : class
        where TThird : class
{
    private readonly IEntitySet<TOuter> _outer;
    private readonly IEntitySet<TInner> _inner;
    private readonly IEntitySet<TThird> _third;
    private readonly Expression _outerKeySelector;
    private readonly Expression _innerKeySelector;
    private readonly Expression _secondJoinKeySelector;
    private readonly Expression _thirdKeySelector;
    private readonly JoinKeySource _secondJoinKeySource;
    private readonly JoinBuilder _joinBuilder;
    private readonly IKsqlContext _context;

    public UnifiedThreeWayJoinResult(
        IEntitySet<TOuter> outer,
        IEntitySet<TInner> inner,
        IEntitySet<TThird> third,
        Expression outerKeySelector,
        Expression innerKeySelector,
        Expression secondJoinKeySelector,
        Expression thirdKeySelector,
        JoinKeySource secondJoinKeySource,
        JoinBuilder joinBuilder,
        IKsqlContext context)
    {
        _outer = outer;
        _inner = inner;
        _third = third;
        _outerKeySelector = outerKeySelector;
        _innerKeySelector = innerKeySelector;
        _secondJoinKeySelector = secondJoinKeySelector;
        _thirdKeySelector = thirdKeySelector;
        _secondJoinKeySource = secondJoinKeySource;
        _joinBuilder = joinBuilder;
        _context = context;
    }

    public IEntitySet<TResult> Select<TResult>(
        Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector) where TResult : class
    {
        var joinExpression = BuildThreeWayJoinExpression(resultSelector);
        var resultEntityModel = CreateResultEntityModel<TResult>();

        return new JoinResultEntitySet<TResult>(
            _context,
            resultEntityModel,
            joinExpression,
            _joinBuilder);
    }

    private Expression BuildThreeWayJoinExpression<TResult>(
        Expression<Func<TOuter, TInner, TThird, TResult>> resultSelector)
    {
        return Expression.Call(
            typeof(System.Linq.Queryable),
            "Select",
            new[] { typeof(object), typeof(TResult) },
            Expression.Constant("THREE_WAY_JOIN_PLACEHOLDER"),
            resultSelector);
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
