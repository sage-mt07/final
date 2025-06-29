
using Kafka.Ksql.Linq.Core.Exceptions;

namespace Kafka.Ksql.Linq.Messaging.Producers.Exception;

/// <summary>
/// Producerプール例外
/// </summary>
public class ProducerPoolException : KafkaMessageBusException
{
    public ProducerPoolException(string message) : base(message) { }
    public ProducerPoolException(string message, System.Exception innerException) : base(message, innerException) { }
}