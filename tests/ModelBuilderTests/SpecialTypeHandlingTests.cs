using System;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Configuration;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.ModelBuilderTests;

public class SpecialTypeHandlingTests
{
    private class CharEntity
    {
        [Key]
        public int Id { get; set; }
        public char Code { get; set; }
    }

    private class ShortEntity
    {
        [Key]
        public int Id { get; set; }
        public short Quantity { get; set; }
    }

    [Fact]
    public void CharProperty_ProducesWarning()
    {
        var builder = new ModelBuilder(ValidationMode.Relaxed);
        builder.Entity<CharEntity>();

        var model = builder.GetEntityModel<CharEntity>()!;
        Assert.Contains(model.ValidationResult!.Warnings, w => w.Contains("char"));
    }

    [Fact]
    public void ShortProperty_MappedAsInteger()
    {
        var generator = new Kafka.Ksql.Linq.Query.Pipeline.DDLQueryGenerator();
        var result = PrivateAccessor.InvokePrivate<string>(generator, "MapToKsqlType", new[] { typeof(Type) }, args: new object?[] { typeof(short) });
        Assert.Equal("INTEGER", result);
    }
}
