using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public class AvroTimestampAttribute : Attribute
{
    /// <summary>
    /// タイムスタンプの精度（デフォルト: ミリ秒）
    /// </summary>
    public TimestampPrecision Precision { get; set; } = TimestampPrecision.Milliseconds;

    /// <summary>
    /// タイムゾーン情報（オプション）
    /// </summary>
    public string? TimeZone { get; set; }

    /// <summary>
    /// ウィンドウ処理でのイベントタイム識別子として使用するかどうか
    /// </summary>
    public bool IsEventTime { get; set; } = true;

    public override string ToString()
    {
        var tz = !string.IsNullOrEmpty(TimeZone) ? $", TimeZone: {TimeZone}" : "";
        return $"AvroTimestamp (Precision: {Precision}, EventTime: {IsEventTime}{tz})";
    }
}
public enum TimestampPrecision
{
    Milliseconds,
    Microseconds,
    Nanoseconds
}
