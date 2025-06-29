using System;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders;

public class BuilderUtilTests
{
    [Fact]
    public void ExtractMemberExpression_UnaryExpression_ReturnsMember()
    {
        Expression<Func<TestEntity, object>> expr = e => (object)e.Id;
        var unary = (UnaryExpression)expr.Body;
        var member = BuilderUtil.ExtractMemberExpression(unary);
        Assert.NotNull(member);
        Assert.Equal("Id", member!.Member.Name);
    }
}
