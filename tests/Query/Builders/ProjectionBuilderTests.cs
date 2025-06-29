using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class ProjectionBuilderTests
{
    [Fact]
    public void Build_NewExpressionWithAlias_ReturnsSelectClause()
    {
        Expression<Func<TestEntity, object>> expr = e => new { e.Id, Renamed = e.Name };
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT Id, Name AS Renamed", result);
    }

    [Fact]
    public void Build_ParameterExpression_ReturnsSelectAll()
    {
        Expression<Func<TestEntity, TestEntity>> expr = e => e;
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT *", result);
    }

    [Fact]
    public void Build_UnsupportedOperator_ThrowsNotSupportedException()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Name ?? "unknown";
        var builder = new ProjectionBuilder();
        Assert.Throws<NotSupportedException>(() => builder.Build(expr.Body));
    }

    [Fact]
    public void Build_ToLowerMethod_ConvertsToFunction()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Name.ToLower();
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT LCASE(Name)", result);
    }

    [Fact]
    public void GetSqlOperator_UnsupportedOperator_Throws()
    {
        var visitorType = typeof(ProjectionBuilder).GetNestedType("ProjectionExpressionVisitor", BindingFlags.NonPublic)!;
        var ex = Assert.Throws<TargetInvocationException>(() =>
            InvokePrivate<string>(visitorType, "GetSqlOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.ArrayIndex));
        Assert.IsType<NotSupportedException>(ex.InnerException);
    }

    [Fact]
    public void Build_CountWithoutSelector_GeneratesCountAll()
    {
        Expression<Func<IGrouping<int, TestEntity>, object>> expr = g => g.Count();
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT COUNT(*)", result);
    }

    [Fact]
    public void Build_SubstringWithLength_GeneratesSubstringFunction()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Name.Substring(1, 3);
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT SUBSTRING(Name, 1, 3)", result);
    }
}
