# Messaging 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **型安全Producer/Consumer管理**：EntityModel基準の型安全なKafka通信
- **Pool削除による直接管理**：複雑性を排除し、Confluent.Kafka完全委譲
- **エラーハンドリング・リトライ機能**：堅牢な障害回復とDLQ送信
- **バッチ処理・ストリーミング対応**：単発送信から大量バッチ、リアルタイム購読まで

### 設計原則・制約
- **EF風API**: DbContextライクな開発体験の提供
- **事前確定管理**: 型・トピック情報の初期化時確定、ランタイム生成なし
- **Fail-Fast**: 初期化時エラーは即座に終了、実行時は適切な例外処理
- **非同期ファースト**: 全I/O操作の非同期化とCancellationToken対応

### 他namespaceとの境界
- **Core**: EntityModelとKafkaMessageContextを受け取り、型安全性を確保
- **Serialization**: シリアライゼーションマネージャーを活用、直接Avro操作なし
- **Configuration**: 設定値に基づくProducer/Consumer設定構築
- **StateStore**: エラー時のDLQ送信、直接的なStateStore操作なし

---

## 🏗️ 主要クラス構成

### 📁 Abstractions/ - インターフェース定義（🟢低変更頻度）
| ファイル | クラス/インターフェース | 責務 | 変更頻度 |
|---------|----------------------|------|----------|
| `IKafkaProducer.cs` | IKafkaProducer<T> | 型安全Producer操作の抽象化 | 🟢 |
| `IKafkaConsumer.cs` | IKafkaConsumer<TValue,TKey> | 型安全Consumer操作の抽象化 | 🟢 |

### 📁 Configuration/ - 設定管理（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `CommonSection.cs` | CommonSection | Kafka共通設定（Bootstrap、SSL等） | 🟡 |
| `ProducerSection.cs` | ProducerSection | Producer専用設定（Acks、圧縮等） | 🟡 |
| `ConsumerSection.cs` | ConsumerSection | Consumer専用設定（GroupId、Offset等） | 🟡 |
| `TopicSection.cs` | TopicSection | トピック別設定（パーティション、レプリケーション等） | 🟡 |
| `SchemaRegistrySection.cs` | SchemaRegistrySection | Schema Registry接続・認証設定 | 🟡 |

### 📁 Producers/ - Producer管理（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KafkaProducerManager.cs` | KafkaProducerManager | 型安全Producer生成・管理・キャッシュ | 🔴 |
| `Core/KafkaProducer.cs` | KafkaProducer<T> | 型安全メッセージ送信・バッチ処理 | 🔴 |
| `DlqProducer.cs` | DlqProducer | DLQ（Dead Letter Queue）送信処理 | 🟡 |
| `Core/PooledProducer.cs` | PooledProducer | Pool管理用Producer（旧実装の名残） | 🟢 |

### 📁 Consumers/ - Consumer管理（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KafkaConsumerManager.cs` | KafkaConsumerManager | 型安全Consumer生成・管理・キャッシュ | 🔴 |
| `Core/KafkaConsumer.cs` | KafkaConsumer<TValue,TKey> | 型安全メッセージ消費・ストリーミング | 🔴 |
| `Core/PooledConsumer.cs` | PooledConsumer | Pool管理用Consumer（旧実装の名残） | 🟢 |

### 📁 Models/ - データモデル（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `DlqEnvelope.cs` | DlqEnvelope | DLQ送信用メッセージ封筒 | 🟢 |

### 📁 Internal/ - 内部機能（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `ErrorHandlingContext.cs` | ErrorHandlingContext | エラー処理・リトライ制御 | 🟡 |

### 📁 Contracts/ - 契約インターフェース（🟢低変更頻度）
| ファイル | インターフェース | 責務 | 変更頻度 |
|---------|-----------------|------|----------|
| `IErrorSink.cs` | IErrorSink | エラーレコード処理の抽象化 | 🟢 |

---

## 🔄 データフロー・依存関係

### Producer側フロー
```
Entity → KafkaProducerManager.SendAsync()
   ↓
KafkaProducer<T>.SendAsync() → KeyExtractor.ExtractKeyValue()
   ↓
IAvroSerializationManager → Serialize
   ↓
Confluent.Kafka.IProducer → Kafka Cluster
   ↓
KafkaDeliveryResult
```

### Consumer側フロー
```
Kafka Cluster → Confluent.Kafka.IConsumer
   ↓
KafkaConsumer<TValue,TKey> → IAvroSerializationManager
   ↓
Deserialize → KafkaMessage<TValue,TKey>
   ↓
EventSet.ConsumeAsync() → Business Logic
```

### エラーハンドリングフロー
```
Exception → ErrorHandlingContext
   ↓
ErrorAction.DLQ → DlqProducer → DLQ Topic
ErrorAction.Retry → Exponential Backoff → Retry
ErrorAction.Skip → Log & Continue
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. Producer/Consumer機能拡張**
- `KafkaProducerManager`: 新しい送信パターン（バッチサイズ制御、条件付き送信等）
- `KafkaConsumer`: 新しい消費パターン（フィルタリング、変換処理等）
- シリアライゼーション形式の追加対応

**2. エラーハンドリング強化**
- 新しいErrorAction種別追加
- より精密なリトライポリシー
- DLQ送信条件の細分化

**3. 設定項目拡張**
- 新しいKafka設定プロパティ対応
- 動的設定変更機能
- 環境別設定プロファイル

### 🟡 中頻度変更パターン
**1. 新しいKafka機能対応**
- Confluent Kafkaクライアントアップデート対応
- 新しい圧縮アルゴリズム
- セキュリティ機能強化

**2. パフォーマンス最適化**
- バッチ処理サイズ調整
- コネクションプール最適化
- メモリ使用量削減

### 🟢 低頻度変更パターン
**1. インターフェース変更**
- IKafkaProducer/IKafkaConsumerのメソッド追加
- 破壊的変更（慎重に実施）

**2. アーキテクチャ変更**
- Pool戦略の見直し（現在は直接管理）
- 新しい通信プロトコル対応

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **Pool削除方針**: 複雑性削減のため、ProducerPool/ConsumerPoolは使用しない
- **事前確定主義**: EntityModel情報は初期化時に確定、動的生成禁止
- **Confluent.Kafka委譲**: 低レベルKafka操作はConfluent.Kafkaライブラリに完全委譲
- **型安全性強制**: Generic型パラメータによる型安全性の強制

### パフォーマンス考慮事項
- **シリアライザキャッシュ**: 型別にシリアライザを事前生成・キャッシュ
- **非同期最適化**: ConfigureAwait(false)の適切な使用
- **リソース管理**: IDisposableの適切な実装とリソースリーク防止
- **バックプレッシャー**: 大量データ処理時のメモリ制御

### セキュリティ・品質制約
- **認証情報保護**: SSL/SASL設定の適切な管理
- **監査ログ**: 重要な操作（送信失敗、DLQ送信等）のログ出力
- **例外透過性**: Kafka例外の適切な伝播とラップ
- **Cancel対応**: CancellationTokenによるGraceful Shutdown

---

## 🔗 他Namespaceとの連携

### Core依存関係
```csharp
// EntityModel: エンティティメタデータの取得
EntityModel entityModel = GetEntityModel<T>();
string topicName = entityModel.TopicAttribute?.TopicName;
PropertyInfo[] keyProperties = entityModel.KeyProperties;

// KafkaMessageContext: メッセージコンテキスト管理
var context = new KafkaMessageContext 
{
    MessageId = Guid.NewGuid().ToString(),
    CorrelationId = ExtractCorrelationId(headers)
};
```

### Serialization連携
```csharp
// 型安全なシリアライゼーションマネージャー取得
var serializationManager = GetOrCreateSerializationManager<T>();
var serializerPair = await serializationManager.GetSerializersAsync();

// デシリアライゼーション実行
var message = _valueDeserializer.Deserialize(valueBytes, false, context);
```

### Configuration連携
```csharp
// 設定値に基づくProducer/Consumer設定構築
var producerConfig = BuildProducerConfig(topicName);
var consumerConfig = BuildConsumerConfig(topicName, subscriptionOptions);

// セキュリティ設定適用
if (_options.Common.SecurityProtocol != SecurityProtocol.Plaintext) {
    config.SecurityProtocol = _options.Common.SecurityProtocol;
}
```

### インターフェース定義
```csharp
// Producer操作
Task<KafkaDeliveryResult> SendAsync(T message, KafkaMessageContext?, CancellationToken);
Task<KafkaBatchDeliveryResult> SendBatchAsync(IEnumerable<T> messages, ...);

// Consumer操作  
IAsyncEnumerable<KafkaMessage<TValue,TKey>> ConsumeAsync(CancellationToken);
Task<KafkaBatch<TValue,TKey>> ConsumeBatchAsync(KafkaBatchOptions, CancellationToken);
```

### 協調動作パターン
1. **EventSet→Messaging**: AddAsync()でのProducer呼び出し
2. **Messaging→Serialization**: 型別SerializationManager取得・実行
3. **Messaging→Configuration**: 設定値に基づく接続パラメータ構築
4. **Messaging→DLQ**: エラー時のDead Letter Queue送信

---

## 💡 実装上の重要なポイント

### Producer管理のベストプラクティス
```csharp
// 型別Producer事前生成・キャッシュ
private readonly ConcurrentDictionary<Type, object> _producers = new();

public async Task<IKafkaProducer<T>> GetProducerAsync<T>() where T : class
{
    if (_producers.TryGetValue(typeof(T), out var cached))
        return (IKafkaProducer<T>)cached;
    
    // 新規作成・キャッシュ
    var producer = await CreateProducerInternal<T>();
    _producers.TryAdd(typeof(T), producer);
    return producer;
}
```

### Consumer購読パターン
```csharp
// 非同期ストリーム処理
await foreach (var kafkaMessage in consumer.ConsumeAsync(cancellationToken))
{
    try 
    {
        await handler(kafkaMessage.Value, kafkaMessage.Context);
    }
    catch (Exception ex) 
    {
        // ErrorHandlingContextでリトライ・DLQ処理
        await errorContext.HandleErrorAsync(kafkaMessage.Value, ex);
    }
}
```

### エラーハンドリング戦略
```csharp
// 段階的エラー処理
switch (ErrorAction) 
{
    case ErrorAction.Retry:
        if (CurrentAttempt <= RetryCount) {
            await Task.Delay(RetryInterval);
            throw; // リトライ実行
        }
        break;
    case ErrorAction.DLQ:
        await ErrorSink.HandleErrorAsync(originalMessage, exception);
        break;
    case ErrorAction.Skip:
        // ログ出力してスキップ
        break;
}
```

### リソース管理
```csharp
// 適切なDispose実装
public void Dispose()
{
    foreach (var producer in _producers.Values)
    {
        if (producer is IDisposable disposable)
            disposable.Dispose();
    }
    _schemaRegistryClient?.Dispose();
}
```