using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Core.Extensions;
using System;
using System.Linq;
using Xunit;

#nullable enable

namespace Kafka.Ksql.Linq.Tests.Core;

public class CoreExtensionsMoreTests
{
    [Topic("t")]
    private class Sample
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [KafkaIgnore]
        public int Ignored { get; set; }
        public int? Optional { get; set; }
    }

    private static EntityModel CreateModel() => new()
    {
        EntityType = typeof(Sample),
        TopicAttribute = new TopicAttribute("t"),
        AllProperties = typeof(Sample).GetProperties(),
        KeyProperties = new[] { typeof(Sample).GetProperty(nameof(Sample.Id))! }
    };

    [Fact]
    public void GetSerializableProperties_ExcludesIgnored()
    {
        var model = CreateModel();
        var props = model.GetSerializableProperties();
        Assert.DoesNotContain(props, p => p.Name == nameof(Sample.Ignored));
        Assert.Contains(props, p => p.Name == nameof(Sample.Name));
    }

    [Fact]
    public void IsKafkaEntity_DetectsProperClass()
    {
        Assert.True(typeof(Sample).IsKafkaEntity());
        Assert.False(typeof(string).IsKafkaEntity());
    }

    [Fact]
    public void IsKafkaIgnored_ReturnsTrueForIgnoredProperty()
    {
        var prop = typeof(Sample).GetProperty(nameof(Sample.Ignored))!;
        Assert.True(prop.IsKafkaIgnored());
    }

    [Fact]
    public void IsNullableProperty_WorksForNullableAndReference()
    {
        var opt = typeof(Sample).GetProperty(nameof(Sample.Optional))!;
        var name = typeof(Sample).GetProperty(nameof(Sample.Name))!;
        Assert.True(opt.IsNullableProperty());
        Assert.False(name.IsNullableProperty());
    }
}
