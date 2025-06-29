using System;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Query.Tests;

public class ProjectionBuilderTests
{
    [Theory]
    [InlineData(ExpressionType.Add, "+")]
    [InlineData(ExpressionType.Multiply, "*")]
    public void GetSqlOperator_ReturnsExpected(ExpressionType type, string expected)
    {
        var visitorType = typeof(ProjectionBuilder).GetNestedType("ProjectionExpressionVisitor", BindingFlags.NonPublic)!;
        var result = InvokePrivate<string>(visitorType, "GetSqlOperator", new[] { typeof(ExpressionType) }, null, type);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Build_ToUpper_UsesFunction()
    {
        Expression<Func<TestEntity, object>> expr = e => e.Name.ToUpper();
        var builder = new ProjectionBuilder();
        var result = builder.Build(expr.Body);
        Assert.Equal("SELECT UCASE(Name)", result);
    }
}
