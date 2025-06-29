# Window 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **ウィンドウ確定処理**: Grace Period経過後のウィンドウ確定足生成
 - **確定足生成・配信**: 集約結果の`{topic}_window_{windowMinutes}_final`トピック送信
- **RocksDB永続化**: 確定足データの永続化とWindow履歴管理
- **重複排除・POD協調**: 複数POD環境での確定足重複排除機能

### 設計原則・制約
- **Grace Period遵守**: ウィンドウ終了 + Grace Period後の確定処理
- **確定足の冪等性**: 同一ウィンドウキーの重複送信対策
- **POD分散協調**: 複数インスタンス環境での安全な確定処理
- **永続化ファースト**: RocksDBによる確定足履歴の確実な保存

### 他namespaceとの境界
- **Core**: WindowedEntitySet等のWindow操作基盤を活用
- **StateStore**: RocksDB連携による確定足永続化
- **Messaging**: 確定足トピック送信用のKafkaProducer使用
- **Application**: WindowFinalizationManagerの初期化・設定
- **Application**: OnModelCreatingでFinalトピックを自動生成

---

## 🏗️ 主要クラス構成

### 📁 Finalization/ - 確定処理コア（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `WindowFinalizationManager.cs` | WindowFinalizationManager | ウィンドウ確定処理スケジューリング | 🔴 |
| `WindowProcessor.cs` | WindowProcessor<T> | エンティティ別ウィンドウ処理 | 🔴 |
| `WindowProcessor.cs` | WindowProcessor (基底) | 抽象ウィンドウ処理 | 🟡 |

### 📁 Finalization/Consumer/ - 確定足消費（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `WindowFinalConsumer.cs` | WindowFinalConsumer | 確定足消費・RocksDB保存 | 🟡 |

### 📁 Finalization/Models/ - データモデル（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `WindowFinalMessage.cs` | WindowFinalMessage | 確定足メッセージ構造 | 🟢 |
| `WindowState.cs` | WindowState<T> | ウィンドウ状態管理 | 🟢 |
| `WindowConfiguration.cs` | WindowConfiguration<T> | ウィンドウ設定情報 | 🟢 |

### 📁 Finalization/Options/ - 設定管理（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `WindowFinalizationOptions.cs` | WindowFinalizationOptions | 確定処理設定オプション | 🟢 |

### 📁 Finalization/Abstractions/ - 抽象化（🟢低変更頻度）
| ファイル | インターフェース | 責務 | 変更頻度 |
|---------|-----------------|------|----------|
| `IKafkaProducer.cs` | IKafkaProducer | Kafka送信抽象化 | 🟢 |

---

## 🔄 データフロー・依存関係

### ウィンドウ確定処理フロー
```
イベント受信 → WindowProcessor<T>.AddToWindow()
   ↓
複数ウィンドウサイズでWindowState<T>管理
   ↓
定期確定チェック → ShouldFinalizeWindow(Grace Period判定)
   ↓
FinalizeWindow() → 集約処理実行
   ↓
WindowFinalMessage生成 → {topic}_window_{windowMinutes}_final送信
   ↓
RocksDB永続化 → 確定足履歴保存
```

### 確定足消費・永続化フロー
```
{topic}_window_{windowMinutes}_final → WindowFinalConsumer.SubscribeToFinalizedWindows()
   ↓
HandleFinalizedWindowWithRocksDB() → 重複排除チェック
   ↓
初回確定足のみ処理 → RocksDB.Put(windowKey, finalMessage)
   ↓
外部ハンドラー実行 → ビジネスロジック処理
```

### POD協調・重複排除フロー
```
複数POD → 同一ウィンドウ確定
   ↓
各PODから{topic}_window_{windowMinutes}_final送信（同一キー）
   ↓
WindowFinalConsumer → 初回到着のみ処理
   ↓
後続の重複メッセージは無視・ログ出力
```

### 履歴検索・参照フロー
```
GetFinalizedWindow(windowKey)
   ↓
メモリキャッシュ確認 → Hit時は即返却
   ↓
RocksDB.Get(windowKey) → 永続化データ取得
   ↓
結果をメモリキャッシュにも保存
```

確定済みデータをアプリケーションから参照する際は、`WindowedEntitySet<T>`
に対して `UseFinalized()` を呼び出すと `orders_window_{windowMinutes}_final`
トピックを読み込む `ReadCachedWindowSet<T>` が返されます。既定では通常の
トピックを使用しますが `KsqlDslOptions.ReadFromFinalTopicByDefault` を
`true` にすると自動的に Final トピックを参照します。

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. 確定処理ロジック改善**
- `WindowFinalizationManager`: スケジューリング間隔調整、並列処理最適化
- `WindowProcessor<T>`: 新しい集約関数対応、Grace Period計算改善
- 確定判定ロジック（`ShouldFinalizeWindow`）の精密化

**2. パフォーマンス最適化**
- バッチ確定処理の実装
- メモリ使用量最適化（古いWindowState清掃）
- RocksDB書き込み最適化

**3. エラーハンドリング強化**
- 確定処理失敗時のリトライ機能
- 部分失敗時の復旧処理
- POD間協調での異常ケース対応

### 🟡 中頻度変更パターン
**1. 確定足消費機能拡張**
- `WindowFinalConsumer`: 新しい検索パターン、フィルタリング機能
- 履歴管理機能（保持期間、圧縮等）
- 統計・モニタリング機能

**2. 設定・構成管理**
- WindowConfigurationの新オプション
- 動的設定変更対応
- 環境別設定プロファイル

**3. RocksDB連携改善**
- バックアップ・リストア機能
- 圧縮・最適化設定
- パーティション対応

### 🟢 低頻度変更パターン
**1. データモデル変更**
- `WindowFinalMessage`: 新フィールド追加（慎重に実施）
- `WindowState<T>`: 状態管理の拡張

**2. インターフェース変更**
- `IKafkaProducer`: 新機能追加（破壊的変更は慎重）

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **Grace Period遵守**: ウィンドウ終了後の待機時間必須
- **確定足の冪等性**: 同一キーの重複送信防止必須
- **POD分散対応**: 複数インスタンス環境での安全性確保
- **永続化保証**: RocksDBによる確定足履歴の確実な保存

### パフォーマンス考慮事項
- **メモリ管理**: WindowStateの適切なライフサイクル管理
- **RocksDB I/O**: 書き込み頻度とフラッシュタイミング最適化
- **タイマー間隔**: 確定処理間隔と精度のバランス
- **POD間通信**: Kafkaトピック経由の効率的な協調

### セキュリティ・品質制約
- **データ整合性**: 確定足の正確性と完全性保証
- **重複排除**: 完全な冪等性確保
- **障害回復**: POD障害時の確定処理継続性
- **リソースリーク防止**: Timer、RocksDB等の適切な解放

---

## 🔗 他Namespaceとの連携

### Core連携
```csharp
// WindowedEntitySetとの統合
public void AddToWindow(T entity, DateTime eventTime)
{
    foreach (var windowMinutes in _config.Windows)
    {
        var windowKey = GenerateWindowKey(entity, eventTime, windowMinutes);
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
            }
        }
    }
}
```

### StateStore連携
```csharp
// RocksDB永続化
public WindowFinalConsumer(
    RocksDbStateStore<string, WindowFinalMessage> rocksDbStore,
    ILoggerFactory? loggerFactory = null)
{
    _rocksDbStore = rocksDbStore;
}

private async Task HandleFinalizedWindowWithRocksDB(WindowFinalMessage message,
    Func<WindowFinalMessage, Task> messageHandler)
{
    // 重複チェック：同一キーの場合は最初に到着したものを採用
    if (_finalizedWindows.TryAdd(message.WindowKey, message))
    {
        // RocksDBに保存
        _rocksDbStore.Put(message.WindowKey, message);
        
        // 外部ハンドラー実行
        await messageHandler(message);
    }
}
```

### Messaging連携
```csharp
// 確定足トピック送信
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
     await _config.FinalTopicProducer.SendAsync(
         topic: $"{_config.TopicName}_window_{windowState.WindowMinutes}_final",
         key: windowKey,
         value: finalTopicMessage);
}
```

### Application連携
```csharp
// WindowFinalizationManagerの初期化
public void RegisterWindowProcessor<T>(WindowConfiguration<T> config) where T : class
{
    var processorKey = GenerateProcessorKey<T>(config);
    
    if (_processors.ContainsKey(processorKey))
    {
        return; // 既に登録済み
    }

    var processor = new WindowProcessor<T>(config, _logger);
    _processors[processorKey] = processor;
}
```

### インターフェース定義
```csharp
// Kafka Producer抽象化
public interface IKafkaProducer : IDisposable
{
    Task SendAsync(string topic, string key, object value);
}

// Window設定
public class WindowConfiguration<T> where T : class
{
    public string TopicName { get; set; }
    public int[] Windows { get; set; }
    public TimeSpan GracePeriod { get; set; } = TimeSpan.FromSeconds(3);
    public int RetentionHours { get; set; } = 24;
    public Func<List<T>, object> AggregationFunc { get; set; }
    public IKafkaProducer FinalTopicProducer { get; set; }
}
```

### 協調動作パターン
1. **Application→Window**: WindowFinalizationManagerの設定・登録
2. **Window→StateStore**: RocksDBによる確定足永続化
3. **Window→Messaging**: 確定足トピック送信
4. **Window→Core**: WindowedEntitySetとの統合動作

---

## 💡 実装上の重要なポイント

### Grace Period による確定判定
```csharp
// ウィンドウ確定判定ロジック
private bool ShouldFinalizeWindow(WindowState<T> windowState, DateTime currentTime)
{
    // Windowの終了時刻 + Grace Period を過ぎた場合に確定
    var finalizeAt = windowState.WindowEnd.Add(_config.GracePeriod);
    return currentTime >= finalizeAt;
}
```

### POD協調による重複排除
```csharp
// 確定足重複排除（初回のみ処理）
private async Task HandleFinalizedWindowWithRocksDB(WindowFinalMessage message,
    Func<WindowFinalMessage, Task> messageHandler)
{
    // 重複チェック：同一キーの場合は最初に到着したものを採用
    if (_finalizedWindows.TryAdd(message.WindowKey, message))
    {
        _logger.LogDebug("Processing new finalized window: {WindowKey} from POD: {PodId}",
            message.WindowKey, message.PodId);

        // RocksDBに保存 + 外部ハンドラー実行
        _rocksDbStore.Put(message.WindowKey, message);
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
```

### ウィンドウキー生成戦略
```csharp
// 一意なウィンドウキー生成
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
```

### 確定処理の失敗回復
```csharp
// 確定処理失敗時のフラグリセット
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
        // 集約データ生成 + 送信
        var finalizedData = _config.AggregationFunc(windowState.Events);
        await SendToFinalTopic(windowKey, finalizedData, windowState);
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
```

### RocksDB履歴検索・管理
```csharp
// 期間指定での確定足検索
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

// ウィンドウサイズ別の確定足取得
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
```

### リソース管理・清掃処理
```csharp
// 古いウィンドウの自動清掃
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
}
```

---

## 🎯 Window層の設計哲学

### 確定足の信頼性
- **Grace Period遵守**: 遅延データを考慮した確実な確定処理
- **冪等性保証**: 重複送信に対する完全な排除機能
- **永続化ファースト**: RocksDBによる確実な履歴保存

### POD分散協調
- **協調的確定処理**: 複数インスタンス環境での安全な動作
- **重複排除**: 同一キーの確定足に対する初回優先処理
- **障害耐性**: 単一POD障害時の処理継続性

### パフォーマンス最適化
- **メモリ効率**: WindowStateの適切なライフサイクル管理
- **I/O最適化**: RocksDB書き込みの効率化
- **リアルタイム性**: 低遅延での確定処理実行

Window層は、ストリーミングデータの時間軸処理において、確実で効率的な確定足生成を実現する重要な機能を提供しています。