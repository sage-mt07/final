using System;

namespace Kafka.Ksql.Linq.Query.Ksql;

internal class KsqlDbException : Exception
{
    public KsqlDbException(string message) : base(message) { }
    public KsqlDbException(string message, Exception innerException) : base(message, innerException) { }
}
