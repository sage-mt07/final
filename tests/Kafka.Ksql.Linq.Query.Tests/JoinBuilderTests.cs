using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Tests;
using Xunit;
using static Kafka.Ksql.Linq.Tests.PrivateAccessor;

namespace Kafka.Ksql.Linq.Query.Tests;

public class JoinClauseBuilderTests
{
    [Fact]
    public void BuildJoinQuery_InvalidArgs_Throws()
    {
        IQueryable<TestEntity> outer = new List<TestEntity>().AsQueryable();
        IQueryable<TestEntity> inner = new List<TestEntity>().AsQueryable();

        // intentionally mismatch join keys using different property counts
        var join = Queryable.Join<TestEntity, TestEntity, object, object>(
            outer,
            inner,
            o => (object)new { o.Id },
            i => (object)new { i.Id, i.Name },
            (o, i) => (object)new { o.Id });

        var builder = new JoinClauseBuilder();
        var call = (MethodCallExpression)join.Expression;
        var ex = Assert.Throws<TargetInvocationException>(
            () => InvokePrivate<string>(builder, "BuildJoinQuery", new[] { typeof(MethodCallExpression) }, null, call));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
    }
}
