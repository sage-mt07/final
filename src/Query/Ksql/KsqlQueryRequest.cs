using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Ksql;

internal class KsqlQueryRequest
{
    public string Ksql { get; set; } = string.Empty;
    public Dictionary<string, object> StreamsProperties { get; set; } = new();
}
