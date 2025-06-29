using Kafka.Ksql.Linq.Core.Attributes;
using Kafka.Ksql.Linq.Core.Abstractions;
using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class DefaultAndMaxLengthAttributeTests
{
    [Fact]
    public void DefaultValueAttribute_StoresValue()
    {
        var attr = new DefaultValueAttribute(123);
        Assert.Equal(123, attr.Value);
        Assert.Contains("123", attr.ToString());
    }

    [Fact]
    public void DefaultValueAttribute_Null_ShowsNullInToString()
    {
        var attr = new DefaultValueAttribute(null);
        Assert.Equal("DefaultValue: null", attr.ToString());
    }

    [Fact]
    public void MaxLengthAttribute_StoresLength()
    {
        var attr = new MaxLengthAttribute(5);
        Assert.Equal(5, attr.Length);
        Assert.Equal("MaxLength: 5", attr.ToString());
    }

    [Fact]
    public void MaxLengthAttribute_InvalidLength_Throws()
    {
        Assert.Throws<ArgumentException>(() => new MaxLengthAttribute(0));
    }
}

public class TopicAttributeTests
{
    [Fact]
    public void Constructor_SetsTopicName()
    {
        var attr = new TopicAttribute("topic1");
        Assert.Equal("topic1", attr.TopicName);
    }

    [Fact]
    public void ToKafkaTopicConfig_BuildsDictionary()
    {
        var attr = new TopicAttribute("topic1")
        {
            Compaction = true,
            MaxMessageBytes = 1024,
            SegmentBytes = 2048
        };

        var cfg = attr.ToKafkaTopicConfig();

        Assert.Equal("compact", cfg["cleanup.policy"]);
        Assert.Equal(attr.RetentionMs, cfg["retention.ms"]);
        Assert.Equal(1024, cfg["max.message.bytes"]);
        Assert.Equal(2048, cfg["segment.bytes"]);
    }

    [Fact]
    public void ToString_IncludesDescription()
    {
        var attr = new TopicAttribute("topic1") { Description = "desc" };
        var str = attr.ToString();
        Assert.Contains("desc", str);
        Assert.Contains("topic1", str);
    }

    [Fact]
    public void Partitioner_Property_RoundTrips()
    {
        var attr = new TopicAttribute("topic1") { Partitioner = "p" };
        Assert.Equal("p", attr.Partitioner);
    }
}
