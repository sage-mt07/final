# StateStore 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **ローカル状態管理**: RocksDBベースの高性能な永続化ストレージ
- **KafkaトピックとStateStoreの双方向同期**: KTable準拠の状態反映
- **ウィンドウ処理**: 時間ベースの状態集約とWindow EntitySet提供
- **Ready状態監視**: Consumer Lagに基づく同期完了判定とヘルスチェック

### 設計原則・制約
- **KTable互換性**: 最新状態のみ保持、Tombstone（削除）メッセージ対応
- **非同期リアルタイム同期**: トピック変更のストレージへの即座反映
- **障害復旧対応**: バインディング自動復旧、Lag監視による状態把握
- **Window操作最適化**: 時間ウィンドウでの効率的な集約処理

### 他namespaceとの境界
- **Core**: EntityModelによる型情報とキー抽出ロジック活用
- **Messaging**: ConsumerManagerからのストリーミングデータ取得
- **Configuration**: StateStore設定（RocksDB、Window等）の適用
- **Query**: Window操作時のDDL生成とksqlDB連携（間接的）

---

## 🏗️ 主要クラス構成

### 📁 Core/ - StateStore基盤（🟡中変更頻度）
| ファイル | クラス/インターフェース | 責務 | 変更頻度 |
|---------|----------------------|------|----------|
| `IStateStore.cs` | IStateStore<TKey,TValue> | StateStore操作の抽象化 | 🟢 |
| `RocksDbStateStore.cs` | RocksDbStateStore<TKey,TValue> | RocksDB実装、永続化・メモリキャッシュ | 🟡 |

### 📁 Management/ - ストア管理（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `StateStoreManager.cs` | StateStoreManager | ストア生成・ライフサイクル管理 | 🔴 |

### 📁 Integration/ - Kafka連携（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `TopicStateStoreBinding.cs` | TopicStateStoreBinding<T> | トピック↔ストア双方向バインディング | 🔴 |
| `StateStoreBindingManager.cs` | StateStoreBindingManager | 複数バインディングの統合管理 | 🔴 |
| `BindingHealthStatus.cs` | BindingHealthStatus | バインディング健全性情報 | 🟡 |

### 📁 Monitoring/ - Ready状態監視（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `ReadyStateMonitor.cs` | ReadyStateMonitor | Consumer Lag監視・Ready判定 | 🟡 |
| `ReadyStateInfo.cs` | ReadyStateInfo | 詳細同期状態情報 | 🟡 |
| `ReadyStateChangedEventArgs.cs` | ReadyStateChangedEventArgs | Ready状態変更イベント | 🟢 |
| `LagUpdatedEventArgs.cs` | LagUpdatedEventArgs | Lag更新イベント | 🟢 |

### 📁 Extensions/ - Window拡張（🟡中変更頻度）
| ファイル | クラス/インターフェース | 責務 | 変更頻度 |
|---------|----------------------|------|----------|
| `IWindowedEntitySet.cs` | IWindowedEntitySet<T> | ウィンドウ付きEntitySet抽象化 | 🟢 |
| `WindowedEntitySet.cs` | WindowedEntitySet<T> | ウィンドウ操作の具体実装 | 🟡 |
| `WindowExtensions.cs` | WindowExtensions | `.Window(minutes)`拡張メソッド | 🟡 |
| `KafkaContextStateStoreExtensions.cs` | StateStore初期化拡張 | Context初期化時のストア準備 | 🟡 |

### 📁 Configuration/ - 設定管理（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `StateStoreOptions.cs` | StateStoreOptions | ストア設定オプション | 🟢 |
| `StateStoreConfiguration.cs` | StateStoreConfiguration | 設定変換・適用 | 🟢 |

### 📁 ルート/ - エンティティ拡張（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `EventSetWithStateStore.cs` | EventSetWithStateStore<T> | StateStore統合EntitySet | 🟡 |

---

## 🔄 データフロー・依存関係

### StateStore初期化フロー
```
KafkaContext.OnModelCreating()
   ↓
StateStoreExtensions.InitializeStateStores()
   ↓
StateStoreManager.InitializeStoresForEntity()
   ↓
RocksDbStateStore<TKey,TValue> 生成・キャッシュ
```

### トピック同期フロー
```
Kafka Topic → KafkaConsumer → TopicStateStoreBinding
   ↓
CreateKafkaMessage() → ExtractKey()
   ↓
StateStore.Put(key, value) or Delete(key)  ※Tombstone対応
   ↓
定期的な Flush() → RocksDB永続化
```

### Ready状態監視フロー
```
ReadyStateMonitor.CheckLag() (5秒間隔)
   ↓
Consumer.QueryWatermarkOffsets() → エンドオフセット取得
Consumer.Position() → 現在位置取得
   ↓
TotalLag = EndOffset - CurrentOffset
   ↓
Lag == 0 → Ready状態達成 → イベント発火
```

### Window操作フロー
```
EntitySet.Window(5) → WindowedEntitySet<T>
   ↓
StateStoreManager.GetOrCreateStore(entityType, windowMinutes)
   ↓
StateStore操作 + 元EntitySetへの委譲
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. バインディング機能強化**
- `TopicStateStoreBinding`: 新しい同期パターン、エラー回復ロジック改善
- `StateStoreBindingManager`: 複数ストア管理、ヘルスチェック精度向上
- 同期パフォーマンス最適化（バッチ処理、並列化等）

**2. ストア管理機能拡張**
- `StateStoreManager`: 新しいストア種別対応、動的設定変更
- 異なるウィンドウサイズの効率的管理
- メモリ使用量監視・制御

**3. Window操作改善**
- より複雑なウィンドウ条件（Sliding、Session等）
- ウィンドウ間でのデータ移行・マージ処理
- 集約関数の拡張

### 🟡 中頻度変更パターン
**1. Ready状態監視精度向上**
- `ReadyStateMonitor`: より細かいLag監視、予測アルゴリズム
- パーティション別状態の詳細管理
- タイムアウト・リトライロジック改善

**2. RocksDB最適化**
- `RocksDbStateStore`: 新しい永続化オプション、圧縮設定
- インデックス戦略の改善
- バックアップ・リストア機能

**3. 設定機能拡張**
- 環境別StateStore設定
- 動的設定変更対応

### 🟢 低頻度変更パターン
**1. インターフェース変更**
- `IStateStore`の新機能追加
- 破壊的変更（慎重に実施）

**2. 新ストレージバックエンド対応**
- RocksDB以外のストレージエンジン
- クラウドストレージ連携

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **KTable互換性**: 最新状態のみ保持、同一キーの古い値は上書き
- **Tombstone対応**: null値メッセージでの削除処理必須
- **非同期リアルタイム**: UI/APIブロッキングなしの状態同期
- **At-least-once保証**: Kafka Consumerの冪等性への依存

### パフォーマンス考慮事項
- **メモリ vs 永続化バランス**: ConcurrentDictionaryとRocksDBの使い分け
- **バッチフラッシュ**: 定期的フラッシュによるI/O最適化
- **Ready状態判定コスト**: 5秒間隔のLag監視によるオーバーヘッド
- **大量データ処理**: Window集約時のメモリ使用量制御

### セキュリティ・品質制約
- **データ整合性**: StateStoreとKafkaトピックの整合性保証
- **障害回復**: バインディング切断時の自動復旧
- **リソースリーク防止**: IDisposableの適切な実装
- **ログ出力**: Ready状態変化、エラー発生時の詳細ログ

---

## 🔗 他Namespaceとの連携

### Core依存関係
```csharp
// EntityModel: キー抽出とメタデータ活用
private string ExtractKey(T entity)
{
    var keyProperties = _entityModel.KeyProperties;
    if (keyProperties.Length == 1) {
        return keyProperties[0].GetValue(entity)?.ToString() ?? Guid.NewGuid().ToString();
    }
    // 複合キー処理
    return string.Join("|", keyProperties.Select(p => p.GetValue(entity)?.ToString() ?? "null"));
}
```

### Messaging連携
```csharp
// ConsumerManagerからのストリーミングデータ取得
var consumer = await _consumerManager.GetConsumerAsync<T>();

await foreach (var kafkaMessage in consumer.ConsumeAsync(cancellationToken))
{
    var key = ExtractKey(kafkaMessage.Value!);
    
    if (kafkaMessage.Value == null) {
        // Tombstone（削除メッセージ）
        _stateStore.Delete(key);
    } else {
        // 通常メッセージ（更新/挿入）
        _stateStore.Put(key, kafkaMessage.Value);
    }
}
```

### Configuration連携
```csharp
// StateStore設定の適用
private StateStoreOptions CreateStoreOptions(EntityConfiguration? config)
{
    return new StateStoreOptions
    {
        StoreType = config?.StoreType ?? "RocksDb",
        EnableCache = config?.EnableCache ?? false,
        Windows = config?.Windows ?? new List<int>(),
        BaseDirectory = Path.Combine(Path.GetTempPath(), "ksqldsl_stores")
    };
}
```

### インターフェース定義
```csharp
// StateStore基本操作
void Put(TKey key, TValue value);
TValue? Get(TKey key);
bool Delete(TKey key);
IEnumerable<KeyValuePair<TKey, TValue>> All();

// Ready状態監視
Task<bool> WaitUntilReadyAsync(TimeSpan? timeout = null);
ReadyStateInfo GetReadyStateInfo();

// Window拡張
IWindowedEntitySet<T> Window<T>(this IEntitySet<T> entitySet, int windowMinutes);
```

### 協調動作パターン
1. **Context→StateStore**: 初期化時のストア準備・設定適用
2. **StateStore→Messaging**: Consumer経由でのストリーミングデータ取得
3. **StateStore→Core**: EntityModel基準のキー抽出・型安全性確保
4. **EventSet→StateStore**: Window操作時の拡張EntitySet提供

---

## 💡 実装上の重要なポイント

### StateStore生成・キャッシュ戦略
```csharp
// 型・ウィンドウ別のストア管理
private readonly ConcurrentDictionary<string, object> _stores = new();

private IStateStore<TKey, TValue> GetOrCreateStoreInternal<TKey, TValue>(
    Type entityType, int windowMinutes = 0)
{
    var storeKey = GenerateStoreKey(entityType, windowMinutes);
    
    if (_stores.TryGetValue(storeKey, out var existingStore))
        return (IStateStore<TKey, TValue>)existingStore;
    
    // Double-checked locking pattern
    lock (_lock) {
        if (_stores.TryGetValue(storeKey, out existingStore))
            return (IStateStore<TKey, TValue>)existingStore;
        
        var store = new RocksDbStateStore<TKey, TValue>(storeName, storeOptions, _loggerFactory);
        _stores.TryAdd(storeKey, store);
        return store;
    }
}
```

### バインディング自動復旧
```csharp
// 自動再試行付きの消費処理
private async Task ConsumeAndUpdateStateStoreWithRetry(CancellationToken cancellationToken)
{
    var retryCount = 0;
    const int maxRetries = 5;
    
    while (!cancellationToken.IsCancellationRequested) {
        try {
            await ConsumeAndUpdateStateStore(cancellationToken);
            retryCount = 0; // 成功時はリセット
        }
        catch (OperationCanceledException) {
            throw;
        }
        catch (Exception ex) {
            retryCount++;
            if (retryCount >= maxRetries) throw;
            
            var delay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, retryCount - 1));
            await Task.Delay(delay, cancellationToken);
        }
    }
}
```

### Ready状態判定
```csharp
// Lag監視による Ready状態判定
private void CheckLag(object? state)
{
    var assignment = _consumer.Assignment;
    var watermarks = _consumer.QueryWatermarkOffsets(assignment.First(), TimeSpan.FromSeconds(10));
    
    _totalLag = 0;
    foreach (var tp in assignment) {
        var currentPosition = _consumer.Position(tp);
        _totalLag += Math.Max(0, watermarks.High - currentPosition);
    }
    
    var isCurrentlyReady = _totalLag == 0 && assignment.Count > 0;
    if (!_isReady && isCurrentlyReady) {
        _isReady = true;
        OnReadyStateChanged(new ReadyStateChangedEventArgs { IsReady = true });
    }
}
```

### Window操作のパフォーマンス最適化
```csharp
// StateStore + ベースEntitySetの効率的な統合
public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
{
    // StateStoreから高速取得
    var storeData = _stateStore.All().Select(kvp => kvp.Value).ToList();
    
    // ベースからの最新データ取得
    var baseData = await _baseEntitySet.ToListAsync(cancellationToken);
    
    // 重複除去してマージ
    return storeData.Concat(baseData).Distinct().ToList();
}
```