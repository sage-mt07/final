using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class JoinClauseBuilderTests
{
    [Fact]
    public void Build_SimpleJoin_ReturnsJoinQuery()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        var expr = outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id, c.Name }).Expression;
        var builder = new JoinClauseBuilder();
        var result = builder.Build(expr);
        Assert.StartsWith("SELECT o.Id, c.Name FROM TestEntity o JOIN ChildEntity c ON o.Id = c.ParentId", result);
    }

    [Fact]
    public void Build_InvalidJoin_ReturnsErrorComment()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();

        // 型引数を明示的に指定して、意図的に不正なJoinを作成
        var expr = outer.Join<TestEntity, ChildEntity, object, object>(
            inner,
            o => new { o.Id },
            c => new { c.ParentId, c.Id },  // 意図的にキー構造を不一致にする
            (o, c) => new { o.Id }
        ).Expression;

        var builder = new JoinClauseBuilder();
        var result = builder.Build(expr);
        Assert.Contains("JOIN構築エラー", result);
    }

    [Fact]
    public void Build_NullExpression_ThrowsArgumentNullException()
    {
        var builder = new JoinClauseBuilder();
        Assert.Throws<ArgumentNullException>(() => builder.Build(null!));
    }

    private class Holder
    {
        public Holder(object value) { Value = value; }
        public object Value { get; }
    }

    private class SettableHolder
    {
        public object? Value { get; set; }
    }

    [Fact]
    public void FindJoinCall_ReturnsMethodCallFromNewExpression()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        var joinExpr = outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id, c.ParentId }).Expression;
        var ctor = typeof(Holder).GetConstructor(new[] { typeof(object) })!;
        var newExpr = Expression.New(ctor, joinExpr);
        var builder = new JoinClauseBuilder();
        var result = InvokePrivate<MethodCallExpression?>(builder, "FindJoinCall", new[] { typeof(Expression) }, null, newExpr);
        Assert.NotNull(result);
    }

    [Fact]
    public void FindJoinCall_ReturnsMethodCallFromLambda()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        Expression<Func<object>> lambda = () => outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id });
        var builder = new JoinClauseBuilder();
        var result = InvokePrivate<MethodCallExpression?>(builder, "FindJoinCall", new[] { typeof(Expression) }, null, lambda);
        Assert.NotNull(result);
    }

    [Fact]
    public void FindJoinCall_ReturnsMethodCallFromMemberInit()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        var joinExpr = outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id }).Expression;
        var memberInit = Expression.MemberInit(
            Expression.New(typeof(SettableHolder)),
            Expression.Bind(typeof(SettableHolder).GetProperty(nameof(SettableHolder.Value))!, joinExpr));
        var builder = new JoinClauseBuilder();
        var result = InvokePrivate<MethodCallExpression?>(builder, "FindJoinCall", new[] { typeof(Expression) }, null, memberInit);
        Assert.NotNull(result);
    }

    [Fact]
    public void FindJoinCall_ReturnsMethodCallFromInvocation()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        Expression<Func<object>> lambda = () => outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id });
        var invoke = Expression.Invoke(lambda);
        var builder = new JoinClauseBuilder();
        var result = InvokePrivate<MethodCallExpression?>(builder, "FindJoinCall", new[] { typeof(Expression) }, null, invoke);
        Assert.NotNull(result);
    }

    [Fact]
    public void BuildJoinQuery_GeneratesJoinStatement()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<ChildEntity> inner = new List<ChildEntity>().AsQueryable();
        var joinExpr = outer.Join(inner, o => o.Id, c => c.ParentId, (o, c) => new { o.Id, c.Name }).Expression as MethodCallExpression;
        var builder = new JoinClauseBuilder();
        var sql = InvokePrivate<string>(builder, "BuildJoinQuery", new[] { typeof(MethodCallExpression) }, null, joinExpr!);
        Assert.StartsWith("SELECT o.Id, c.Name FROM TestEntity o JOIN ChildEntity c ON o.Id = c.ParentId", sql);
    }
}

