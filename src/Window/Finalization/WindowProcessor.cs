using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
namespace Kafka.Ksql.Linq.Window.Finalization;

internal class WindowProcessor<T> : WindowProcessor where T : class
{
    private readonly WindowConfiguration<T> _config;
    private readonly ConcurrentDictionary<string, WindowState<T>> _windowStates = new();
    private readonly ILogger _logger;

    public WindowProcessor(WindowConfiguration<T> config, ILogger logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// 新しいデータを受信してWindowに追加
    /// </summary>
    public void AddToWindow(T entity, DateTime eventTime)
    {
        foreach (var windowMinutes in _config.Windows)
        {
            var windowKey = GenerateWindowKey(entity, eventTime, windowMinutes);
            var windowStart = CalculateWindowStart(eventTime, windowMinutes);
            var windowEnd = windowStart.AddMinutes(windowMinutes);

            var windowState = _windowStates.GetOrAdd(windowKey, _ => new WindowState<T>
            {
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                WindowMinutes = windowMinutes,
                Events = new List<T>(),
                IsFinalized = false
            });

            lock (windowState.Lock)
            {
                if (!windowState.IsFinalized)
                {
                    windowState.Events.Add(entity);
                    windowState.LastUpdated = DateTime.UtcNow;

                    _logger.LogTrace("Added event to window: {WindowKey}, Events: {Count}",
                        windowKey, windowState.Events.Count);
                }
            }
        }
    }

    /// <summary>
    /// Window確定処理
    /// </summary>
    public override async Task ProcessFinalization(DateTime currentTime)
    {
        var windowsToFinalize = new List<(string key, WindowState<T> state)>();

        // 確定対象Windowを特定
        foreach (var kvp in _windowStates)
        {
            var windowState = kvp.Value;
            if (!windowState.IsFinalized && ShouldFinalizeWindow(windowState, currentTime))
            {
                windowsToFinalize.Add((kvp.Key, windowState));
            }
        }

        // 確定処理実行
        foreach (var (windowKey, windowState) in windowsToFinalize)
        {
            await FinalizeWindow(windowKey, windowState);
        }

        // 古いWindowの清掃
        await CleanupOldWindows(currentTime);
    }

    /// <summary>
    /// Window確定判定
    /// </summary>
    private bool ShouldFinalizeWindow(WindowState<T> windowState, DateTime currentTime)
    {
        // Windowの終了時刻 + Grace Period を過ぎた場合に確定
        var finalizeAt = windowState.WindowEnd.Add(_config.GracePeriod);
        return currentTime >= finalizeAt;
    }

    /// <summary>
    /// Window確定実行
    /// </summary>
    private async Task FinalizeWindow(string windowKey, WindowState<T> windowState)
    {
        lock (windowState.Lock)
        {
            if (windowState.IsFinalized)
            {
                return; // 既に確定済み
            }

            windowState.IsFinalized = true;
        }

        try
        {
            // 集約データ生成
            var finalizedData = _config.AggregationFunc(windowState.Events);

            // orders_window_final トピックに送信
            await SendToFinalTopic(windowKey, finalizedData, windowState);

            _logger.LogInformation("Finalized window: {WindowKey}, Events: {EventCount}, " +
                                 "Window: {Start} - {End}",
                windowKey, windowState.Events.Count,
                windowState.WindowStart, windowState.WindowEnd);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to finalize window: {WindowKey}", windowKey);

            // 確定フラグをリセット（再試行可能にする）
            lock (windowState.Lock)
            {
                windowState.IsFinalized = false;
            }
        }
    }

    /// <summary>
    /// 確定足を最終トピックに送信
    /// </summary>
    private async Task SendToFinalTopic(string windowKey, object finalizedData, WindowState<T> windowState)
    {
        var finalTopicMessage = new WindowFinalMessage
        {
            WindowKey = windowKey,
            WindowStart = windowState.WindowStart,
            WindowEnd = windowState.WindowEnd,
            WindowMinutes = windowState.WindowMinutes,
            EventCount = windowState.Events.Count,
            AggregatedData = finalizedData,
            FinalizedAt = DateTime.UtcNow,
            PodId = Environment.MachineName // POD識別用
        };

        // KafkaProducerを使用して送信
        // 重複送信対策：同一キーの場合は最初に到着したものが有効
        var finalTopic = _config.GetFinalTopicName(windowState.WindowMinutes);

        await _config.FinalTopicProducer.SendAsync(
            topic: finalTopic,
            key: windowKey,
            value: finalTopicMessage);

        _logger.LogDebug("Sent finalized window to topic: {Topic}, Key: {Key}",
            finalTopic, windowKey);
    }

    /// <summary>
    /// 古いWindowの清掃
    /// </summary>
    private async Task CleanupOldWindows(DateTime currentTime)
    {
        var cleanupThreshold = currentTime.AddHours(-_config.RetentionHours);
        var keysToRemove = new List<string>();

        foreach (var kvp in _windowStates)
        {
            var windowState = kvp.Value;
            if (windowState.IsFinalized && windowState.WindowEnd < cleanupThreshold)
            {
                keysToRemove.Add(kvp.Key);
            }
        }

        foreach (var key in keysToRemove)
        {
            _windowStates.TryRemove(key, out _);
        }

        if (keysToRemove.Count > 0)
        {
            _logger.LogDebug("Cleaned up {Count} old windows for entity {EntityType}",
                keysToRemove.Count, typeof(T).Name);
        }

        await Task.CompletedTask;
    }

    private string GenerateWindowKey(T entity, DateTime eventTime, int windowMinutes)
    {
        var windowStart = CalculateWindowStart(eventTime, windowMinutes);
        var entityKey = ExtractEntityKey(entity);
        return $"{entityKey}_{windowStart:yyyyMMddHHmm}_{windowMinutes}min";
    }

    private DateTime CalculateWindowStart(DateTime eventTime, int windowMinutes)
    {
        var totalMinutes = eventTime.Hour * 60 + eventTime.Minute;
        var windowStartMinutes = (totalMinutes / windowMinutes) * windowMinutes;
        var hours = windowStartMinutes / 60;
        var minutes = windowStartMinutes % 60;

        return new DateTime(eventTime.Year, eventTime.Month, eventTime.Day, hours, minutes, 0);
    }

    private string ExtractEntityKey(T entity)
    {
        // エンティティのキープロパティから一意キーを生成
        if (entity == null) return Guid.NewGuid().ToString();

        var keyProperties = typeof(T).GetProperties()
            .Where(p => p.GetCustomAttribute<KeyAttribute>() != null)
            .OrderBy(p => p.GetCustomAttribute<KeyAttribute>()?.Order ?? 0);

        if (!keyProperties.Any())
        {
            return entity.GetHashCode().ToString();
        }

        var keyParts = keyProperties.Select(p => p.GetValue(entity)?.ToString() ?? "null");
        return string.Join("|", keyParts);
    }

    public override void Dispose()
    {
        _windowStates.Clear();
    }
}

internal abstract class WindowProcessor : IDisposable
{
    public abstract Task ProcessFinalization(DateTime currentTime);
    public abstract void Dispose();
}
