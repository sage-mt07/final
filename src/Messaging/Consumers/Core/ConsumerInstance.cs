using Kafka.Ksql.Linq.Core.Abstractions;
using System;

namespace Kafka.Ksql.Linq.Messaging.Consumers.Core;
internal class ConsumerInstance
{
    public ConsumerKey ConsumerKey { get; set; } = default!;
    public PooledConsumer PooledConsumer { get; set; } = default!;
    public DateTime RentedAt { get; set; }
    public bool IsActive { get; set; }
}
