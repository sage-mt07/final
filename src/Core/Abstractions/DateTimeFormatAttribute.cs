using System;
using System.Diagnostics.CodeAnalysis;

namespace Kafka.Ksql.Linq.Core.Abstractions;

[AttributeUsage(AttributeTargets.Property)]
public class DateTimeFormatAttribute : Attribute
{
    [SetsRequiredMembers]
    public DateTimeFormatAttribute(string format = "yyyy-MM-dd HH:mm:ss")
    {
        Format = format ?? throw new ArgumentNullException(nameof(format));
    }

    public required string Format { get; set; }
    public string? Region { get; set; }
}
