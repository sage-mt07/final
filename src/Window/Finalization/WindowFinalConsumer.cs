using Kafka.Ksql.Linq.StateStore.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Window.Finalization;

internal class WindowFinalConsumer : IDisposable
{
    private readonly ILogger<WindowFinalConsumer> _logger;
    private readonly ConcurrentDictionary<string, WindowFinalMessage> _finalizedWindows = new();
    private readonly RocksDbStateStore<string, WindowFinalMessage> _rocksDbStore;
    private bool _disposed = false;

    public WindowFinalConsumer(
        RocksDbStateStore<string, WindowFinalMessage> rocksDbStore,
        ILoggerFactory? loggerFactory = null)
    {
        _rocksDbStore = rocksDbStore ?? throw new ArgumentNullException(nameof(rocksDbStore));
        _logger = loggerFactory?.CreateLogger<WindowFinalConsumer>()
                 ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<WindowFinalConsumer>.Instance;
    }

    /// <summary>
    /// 確定足データを購読してRocksDBに保存
    /// </summary>
    public async Task SubscribeToFinalizedWindows(string topicName,
        int windowMinutes,
        Func<WindowFinalMessage, Task> messageHandler)
    {
        _logger.LogInformation("Starting subscription to finalized windows: {Topic}({Window}) → RocksDB",
            topicName, windowMinutes);

        var finalTopic = $"{topicName}_window_{windowMinutes}_final";

        // Kafka Consumer setup for final topic
        // await foreach (var message in kafkaConsumer.ConsumeAsync())
        // {
        //     await HandleFinalizedWindowWithRocksDB(message, messageHandler);
        // }

        await Task.CompletedTask; // 仮実装
    }

    /// <summary>
    /// 確定足メッセージ処理（重複排除 + RocksDB保存）
    /// </summary>
    private async Task HandleFinalizedWindowWithRocksDB(WindowFinalMessage message,
        Func<WindowFinalMessage, Task> messageHandler)
    {
        // 重複チェック：同一キーの場合は最初に到着したものを採用
        if (_finalizedWindows.TryAdd(message.WindowKey, message))
        {
            _logger.LogDebug("Processing new finalized window: {WindowKey} from POD: {PodId}",
                message.WindowKey, message.PodId);

            // RocksDBに保存
            _rocksDbStore.Put(message.WindowKey, message);

            // 外部ハンドラー実行
            await messageHandler(message);
        }
        else
        {
            var existingMessage = _finalizedWindows[message.WindowKey];
            _logger.LogDebug("Duplicate finalized window ignored: {WindowKey}. " +
                           "Existing from POD: {ExistingPod}, Duplicate from POD: {DuplicatePod}",
                message.WindowKey, existingMessage.PodId, message.PodId);
        }
    }

    /// <summary>
    /// 過去の確定足データ参照（RocksDB優先）
    /// </summary>
    public WindowFinalMessage? GetFinalizedWindow(string windowKey)
    {
        // まずメモリキャッシュを確認
        if (_finalizedWindows.TryGetValue(windowKey, out var cachedWindow))
        {
            return cachedWindow;
        }

        // RocksDBから取得
        var persistedWindow = _rocksDbStore.Get(windowKey);
        if (persistedWindow != null)
        {
            // メモリキャッシュにも保存
            _finalizedWindows.TryAdd(windowKey, persistedWindow);
            return persistedWindow;
        }

        return null;
    }

    /// <summary>
    /// 指定期間の確定足データ取得（RocksDB検索）
    /// </summary>
    public List<WindowFinalMessage> GetFinalizedWindowsInRange(DateTime start, DateTime end)
    {
        var results = new List<WindowFinalMessage>();

        // RocksDBから全データを取得して期間フィルタ
        foreach (var kvp in _rocksDbStore.All())
        {
            var window = kvp.Value;
            if (window.WindowStart >= start && window.WindowEnd <= end)
            {
                results.Add(window);
            }
        }

        return results.OrderBy(w => w.WindowStart).ToList();
    }

    /// <summary>
    /// 特定ウィンドウサイズの確定足データ取得
    /// </summary>
    public List<WindowFinalMessage> GetFinalizedWindowsBySize(int windowMinutes, DateTime? since = null)
    {
        var results = new List<WindowFinalMessage>();
        var cutoffTime = since ?? DateTime.UtcNow.AddDays(-7); // デフォルト7日前

        foreach (var kvp in _rocksDbStore.All())
        {
            var window = kvp.Value;
            if (window.WindowMinutes == windowMinutes && window.WindowStart >= cutoffTime)
            {
                results.Add(window);
            }
        }

        return results.OrderBy(w => w.WindowStart).ToList();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            // RocksDBをフラッシュしてから解放
            _rocksDbStore?.Flush();
            _rocksDbStore?.Dispose();

            _finalizedWindows.Clear();
            _logger.LogInformation("WindowFinalConsumer disposed with RocksDB persistence");
        }
    }
}
