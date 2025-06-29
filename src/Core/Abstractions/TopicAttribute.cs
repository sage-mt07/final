using Kafka.Ksql.Linq.Core.Attributes;
using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Core.Abstractions;
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public class TopicAttribute : Attribute
{
    public string TopicName { get; }

    [DefaultValue(1)]
    public int PartitionCount { get; set; } = 1;

    [DefaultValue(1)]
    public int ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// ISR最小数。Kafkaのトピック可用性を保つには必須
    /// </summary>
    public int? MinInSyncReplicas { get; set; }

    [DefaultValue(604800000)]
    public long RetentionMs { get; set; } = 604800000; // 7 days

    public bool Compaction { get; set; } = false;

    public bool DeadLetterQueue { get; set; } = false;

    /// <summary>
    /// カスタムパーティショニング戦略名
    /// </summary>
    public string? Partitioner { get; set; }

    public string? Description { get; set; }

    public int? MaxMessageBytes { get; set; }

    public long? SegmentBytes { get; set; }

    public TopicAttribute(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("トピック名は必須です", nameof(topicName));

        TopicName = topicName;
    }

    public override string ToString()
    {
        var desc = string.IsNullOrEmpty(Description) ? "" : $" ({Description})";
        return $"Topic: {TopicName}{desc}, Partitions: {PartitionCount}, Replicas: {ReplicationFactor}";
    }
    public Dictionary<string, object> ToKafkaTopicConfig()
    {
        var config = new Dictionary<string, object>
        {
            ["cleanup.policy"] = Compaction ? "compact" : "delete",
            ["retention.ms"] = RetentionMs
        };

        if (MaxMessageBytes.HasValue)
            config["max.message.bytes"] = MaxMessageBytes.Value;

        if (SegmentBytes.HasValue)
            config["segment.bytes"] = (int)SegmentBytes.Value;

        if (MinInSyncReplicas.HasValue)
            config["min.insync.replicas"] = MinInSyncReplicas.Value;

        return config;
    }
}
