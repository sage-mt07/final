using Confluent.Kafka;
using System;

namespace Kafka.Ksql.Linq.Messaging.Producers.Core;
// =============================================================================
// Pool & Internal Classes - プール・内部管理クラス
// =============================================================================

/// <summary>
/// プールされたProducer
/// </summary>
internal class PooledProducer
{
    public IProducer<object, object> Producer { get; set; } = default!;
    public DateTime CreatedAt { get; set; }
    public DateTime LastUsed { get; set; }
    public int UsageCount { get; set; }
    public bool IsHealthy { get; set; } = true;
}