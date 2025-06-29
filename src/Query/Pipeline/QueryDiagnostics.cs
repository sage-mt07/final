using Kafka.Ksql.Linq.Core.Extensions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// クエリ診断・デバッグ情報管理 - QueryExecutionPipeline用
/// 設計理由：新アーキテクチャでの診断機能提供、既存QueryDiagnosticsを置き換え
/// </summary>
internal class QueryDiagnostics
{
    private readonly Dictionary<string, object> _metadata = new();
    private readonly ILogger _logger;
    private DateTime _startTime;
    private TimeSpan _analysisTime;

    public Dictionary<string, object> MetaData => _metadata;

    public QueryDiagnostics(ILoggerFactory? loggerFactory = null)
    {
        _startTime = DateTime.UtcNow;
        _logger = loggerFactory.CreateLoggerOrNull<QueryDiagnostics>();
        _logger.LogDebug("QueryDiagnostics initialized at {StartTime}", _startTime);
    }

    /// <summary>
    /// 解析ステップを記録
    /// </summary>
    public void LogStep(string step, object? detail = null)
    {
        var timestamp = DateTime.UtcNow.ToString("HH:mm:ss.fff");
        var message = $"[{timestamp}] {step}";

        if (detail != null)
        {
            message += $": {detail}";
        }

        _logger.LogDebug("Diagnostics step: {Step}", message);
    }

    /// <summary>
    /// メタデータ設定
    /// </summary>
    public void SetMetadata(string key, object value)
    {
        _metadata[key] = value;
        _logger.LogDebug("Diagnostics metadata set: {Key} = {Value}", key, value);
    }

    /// <summary>
    /// 解析完了マーク
    /// </summary>
    public void MarkComplete()
    {
        _analysisTime = DateTime.UtcNow - _startTime;
        _logger.LogDebug("Query analysis completed in {AnalysisTimeMs:F2}ms", _analysisTime.TotalMilliseconds);
    }

    // Debug reporting APIs removed in Phase 1 cleanup
}