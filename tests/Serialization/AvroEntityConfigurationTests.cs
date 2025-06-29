using System;
using System.Linq;
using Kafka.Ksql.Linq.Core.Abstractions;
using Kafka.Ksql.Linq.Serialization.Abstractions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Serialization;

public class AvroEntityConfigurationTests
{
    [Topic("orders")]
    private class Order
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        [KafkaIgnore]
        public string Debug { get; set; } = string.Empty;
    }

    [Fact]
    public void Constructor_AppliesAttributes()
    {
        var cfg = new AvroEntityConfiguration(typeof(Order));
        Assert.Equal("orders", cfg.TopicName);
        Assert.True(cfg.HasKeys());
    }

    [Fact]
    public void GetSerializableProperties_ExcludesIgnored()
    {
        var cfg = new AvroEntityConfiguration(typeof(Order));
        var props = cfg.GetSerializableProperties();
        Assert.DoesNotContain(typeof(Order).GetProperty(nameof(Order.Debug))!, props);
    }

    [Fact]
    public void DetermineKeyType_ReturnsPropertyType()
    {
        var cfg = new AvroEntityConfiguration(typeof(Order));
        Assert.Equal(typeof(int), cfg.DetermineKeyType());
    }

    [Fact]
    public void GetSummary_IncludesInfo()
    {
        var cfg = new AvroEntityConfiguration(typeof(Order));
        var summary = cfg.GetSummary();
        Assert.Contains("Order", summary);
        Assert.Contains("orders", summary);
    }
    private class MultiKey
    {
        [Key(Order = 2)] public int B { get; set; }
        [Key(Order = 1)] public int A { get; set; }
        [KafkaIgnore] public string Skip { get; set; } = string.Empty;
    }

    [Fact]
    public void Clone_CreatesEqualCopy()
    {
        var cfg = new AvroEntityConfiguration(typeof(MultiKey));
        var clone = cfg.Clone();
        Assert.True(cfg.Equals(clone));
        Assert.Equal(cfg.GetHashCode(), clone.GetHashCode());
        Assert.NotSame(cfg, clone);
    }

    [Fact]
    public void GetOrderedKeyProperties_SortsByOrder()
    {
        var cfg = new AvroEntityConfiguration(typeof(MultiKey));
        var props = cfg.GetOrderedKeyProperties();
        Assert.Equal(new[] { "A", "B" }, props.Select(p => p.Name));
    }

    [Fact]
    public void IsCompositeKey_ReturnsTrue()
    {
        var cfg = new AvroEntityConfiguration(typeof(MultiKey));
        Assert.True(cfg.IsCompositeKey());
    }

    [Fact]
    public void GetIgnoredProperties_ReturnsIgnored()
    {
        var cfg = new AvroEntityConfiguration(typeof(MultiKey));
        var props = cfg.GetIgnoredProperties();
        Assert.Contains(props, p => p.Name == "Skip");
    }

    [Fact]
    public void ToString_ReturnsSummary()
    {
        var cfg = new AvroEntityConfiguration(typeof(MultiKey));
        Assert.Contains("MultiKey", cfg.ToString());
    }

    [Fact]
    public void Validate_WarnsOnBadKeyType()
    {
        var cfg = new AvroEntityConfiguration(typeof(BadKey));
        var res = cfg.Validate();
        Assert.NotEmpty(res.Warnings);
    }

    private class BadKey
    {
        [Key] public DateTime Dt { get; set; }
    }
}
