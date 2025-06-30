using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class HavingClauseBuilderTests
{
    [Fact]
    public void Build_CountWithoutSelector_ReturnsCountAll()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Count() > 1;
        var builder = new HavingClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("HAVING (COUNT(*) > 1)", result);
    }

    [Fact]
    public void Build_SumWithLambda_ReturnsAggregateCondition()
    {
        Expression<Func<IGrouping<int, TestEntity>, bool>> expr = g => g.Sum(x => x.Id) > 5;
        var builder = new HavingClauseBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("HAVING (SUM(Id) > 5)", result);
    }

    [Fact]
    public void Build_NullExpression_ThrowsArgumentNullException()
    {
        var builder = new HavingClauseBuilder();
        Assert.Throws<ArgumentNullException>(() => builder.Build(null!));
    }

    [Theory]
    [InlineData("LATESTBYOFFSET", "LATEST_BY_OFFSET")]
    [InlineData("EARLIESTBYOFFSET", "EARLIEST_BY_OFFSET")]
    [InlineData("COLLECTLIST", "COLLECT_LIST")]
    [InlineData("COLLECTSET", "COLLECT_SET")]
    [InlineData("AVERAGE", "AVG")]
    public void TransformMethodName_ReturnsExpected(string original, string expected)
    {
        var visitorType = typeof(HavingClauseBuilder).GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<string>(visitorType, "TransformMethodName", new[] { typeof(string) }, null, original);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(ExpressionType.Equal, "=")]
    [InlineData(ExpressionType.AndAlso, "AND")]
    public void GetSqlOperator_ReturnsExpected(ExpressionType type, string expected)
    {
        var visitorType = typeof(HavingClauseBuilder).GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<string>(visitorType, "GetSqlOperator", new[] { typeof(ExpressionType) }, null, type);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetSqlOperator_Unsupported_Throws()
    {
        var visitorType = typeof(HavingClauseBuilder).GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var ex = Assert.Throws<TargetInvocationException>(() =>
            InvokePrivate<string>(visitorType, "GetSqlOperator", new[] { typeof(ExpressionType) }, null, ExpressionType.ArrayIndex));
        Assert.IsType<NotSupportedException>(ex.InnerException);
    }

    [Theory]
    [InlineData("SUM", true)]
    [InlineData("COUNT", true)]
    [InlineData("COLLECTLIST", true)]
    [InlineData("UNKNOWN", false)]
    public void IsAggregateFunction_DetectsAggregateMethods(string name, bool expected)
    {
        var visitorType = typeof(HavingClauseBuilder).GetNestedType("HavingExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<bool>(visitorType, "IsAggregateFunction", new[] { typeof(string) }, null, name);
        Assert.Equal(expected, result);
    }
}
