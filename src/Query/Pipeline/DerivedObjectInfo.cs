using System;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal class DerivedObjectInfo
{
    public string Name { get; set; } = string.Empty;
    public DerivedObjectType Type { get; set; }
    public string BaseObject { get; set; } = string.Empty;
    public Expression Expression { get; set; } = Expression.Empty();
    public DateTime CreatedAt { get; set; }
}
