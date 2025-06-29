# Serialization 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **Avroスキーマ生成・登録**: POCO型からAvroスキーマ自動生成とSchema Registry一括登録
- **型安全シリアライゼーション**: ジェネリクス活用による型安全なSerializer/Deserializer管理
- **キャッシュ・パフォーマンス最適化**: エンティティ別Serializerキャッシュとバージョン管理
- **スキーマバージョニング・互換性**: 後方互換性チェックとスキーマ進化対応

### 設計原則・制約
- **POCO属性駆動**: `[KafkaIgnore]`, `[AvroTimestamp]`, `[DecimalPrecision]`等の属性ベース設定
- **自動生成重視**: 手動スキーマ定義なし、型からの完全自動生成
- **型安全性確保**: `IAvroSerializationManager<T>`による型指定管理
- **キャッシュファースト**: 初回生成後のキャッシュ活用によるパフォーマンス最適化

### 他namespaceとの境界
- **Core**: EntityModelとAvroEntityConfigurationの変換・活用
- **Application**: スキーマ一括登録サービスの提供
- **Messaging**: Serializer/DeserializerペアのProducer/Consumer提供
- **Configuration**: Schema Registry接続設定の適用

---

## 🏗️ 主要クラス構成

### 📁 Abstractions/ - 抽象化定義（🟢低変更頻度）
| ファイル | クラス/インターフェース | 責務 | 変更頻度 |
|---------|----------------------|------|----------|
| `AvroEntityConfiguration.cs` | AvroEntityConfiguration | エンティティ設定情報、属性自動読取 | 🟢 |
| `AvroEntityConfigurationBuilder.cs` | AvroEntityConfigurationBuilder<T> | Fluent API設定ビルダー | 🟢 |
| `AvroEntityConfigurationExtensions.cs` | 拡張メソッド | 設定便利メソッド | 🟢 |
| `AvroSerializationManager.cs` | AvroSerializationManager<T> | 型安全シリアライゼーション管理 | 🟡 |
| `IAvroSerializationManager.cs` | IAvroSerializationManager<T> | シリアライゼーション抽象化 | 🟢 |
| `interfaces.cs` | SerializerPair, DeserializerPair等 | データ構造定義 | 🟢 |

### 📁 Avro/Core/ - Avro基盤（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `AvroSchemaInfo.cs` | AvroSchemaInfo | スキーマメタデータ管理 | 🟡 |
| `AvroSerializerFactory.cs` | AvroSerializerFactory | Serializer/Deserializer生成工場 | 🟡 |
| `UnifiedSchemaGenerator.cs` | UnifiedSchemaGenerator | 統合スキーマ生成エンジン | 🔴 |
| `AvroSerializer.cs` | AvroSerializer<T> | カスタムAvroシリアライザー | 🟢 |
| `AvroDeserializer.cs` | AvroDeserializer<T> | カスタムAvroデシリアライザー | 🟢 |
| `SchemaRegistryClientWrapper.cs` | SchemaRegistryClientWrapper | Schema Registry API ラッパー | 🟡 |

### 📁 Avro/Cache/ - キャッシュ機能（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `AvroSerializerCache.cs` | AvroSerializerCache | エンティティ別Serializerキャッシュ | 🔴 |
| `AvroSerializerCache.cs` | AvroEntitySerializationManager<T> | エンティティ専用シリアライゼーション管理 | 🔴 |

### 📁 Avro/Management/ - 管理機能（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `AvroSchemaRegistrationService.cs` | AvroSchemaRegistrationService | 全スキーマ一括登録サービス | 🔴 |
| `AvroSchemaBuilder.cs` | AvroSchemaBuilder | スキーマ生成プロバイダー | 🔴 |
| `AvroSchemaVersionManager.cs` | AvroSchemaVersionManager | スキーマバージョン・互換性管理 | 🟡 |
| `AvroSchemaRepository.cs` | AvroSchemaRepository | スキーマ情報リポジトリ | 🟡 |

### 📁 Avro/Exceptions/ - 例外処理（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `AvroSchemaRegistrationException.cs` | AvroSchemaRegistrationException | スキーマ登録例外 | 🟡 |
| `SchemaRegistrationFatalException.cs` | SchemaRegistrationFatalException | Fail-Fast致命的例外 | 🟡 |

### 📁 Avro/ - 統合機能（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `ResilientAvroSerializerManager.cs` | ResilientAvroSerializerManager | リトライ・障害回復機能 | 🔴 |

---

## 🔄 データフロー・依存関係

### スキーマ生成・登録フロー
```
POCO型定義 → AvroEntityConfiguration.AutoConfigureFromAttributes()
   ↓
UnifiedSchemaGenerator.GenerateTopicSchemas()
   ↓
AvroSchemaRegistrationService.RegisterAllSchemasAsync()
   ↓
SchemaRegistryClient.RegisterSchemaAsync() → Schema Registry
   ↓
AvroSchemaInfo保存・キャッシュ
```

### Serializer取得・キャッシュフロー
```
IAvroSerializationManager<T>.GetSerializersAsync()
   ↓
AvroSerializerCache.GetAvroManager<T>()
   ↓
Cache Hit ? キャッシュ返却 : 新規作成
   ↓
AvroSerializerFactory.CreateSerializersAsync()
   ↓
SerializerPair<T>返却 + キャッシュ保存
```

### 型安全シリアライゼーションフロー
```
Producer: Entity → IAvroSerializationManager<T> → SerializerPair → byte[]
Consumer: byte[] → IAvroSerializationManager<T> → DeserializerPair → Entity
```

### スキーマバージョン管理フロー
```
AvroSchemaVersionManager.CanUpgradeAsync()
   ↓
SchemaRegistry.IsCompatibleAsync() → 互換性チェック
   ↓
UpgradeAsync() → 新スキーマ登録
   ↓
キャッシュクリア・再構築
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. スキーマ生成エンジン拡張**
- `UnifiedSchemaGenerator`: 新しい型対応、属性サポート拡張
- 新しいAvro論理型（decimal、timestamp等）対応
- POCO属性（`[KafkaIgnore]`, `[DecimalPrecision]`等）の機能拡張

**2. キャッシュ・パフォーマンス最適化**
- `AvroSerializerCache`: キャッシュ戦略改善、メモリ効率化
- `AvroEntitySerializationManager`: 統計情報拡張、モニタリング強化
- Round-trip検証、パフォーマンス測定機能

**3. スキーマ登録・管理機能**
- `AvroSchemaRegistrationService`: 一括登録ロジック改善
- `AvroSchemaBuilder`: スキーマ生成オプション拡張
- Fail-Fast例外処理の精密化

### 🟡 中頻度変更パターン
**1. スキーマバージョニング強化**
- `AvroSchemaVersionManager`: より精密な互換性チェック
- スキーマ進化ルールの拡張
- バージョン履歴管理機能

**2. Schema Registry連携改善**
- `SchemaRegistryClientWrapper`: 新しいAPI対応
- 認証・セキュリティ機能強化
- エラーハンドリング改善

**3. 設定・構成管理**
- `AvroEntityConfiguration`: 新しい設定項目追加
- Builder APIの機能拡張

### 🟢 低頻度変更パターン
**1. 抽象化インターフェース変更**
- `IAvroSerializationManager`: 新機能追加（破壊的変更は慎重）
- データ構造（SerializerPair等）の拡張

**2. 新しいシリアライゼーション形式対応**
- Avro以外の形式（JSON、Protobuf等）への対応
- フォーマット選択機能

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **POCO属性主導**: 手動スキーマ定義禁止、属性ベース自動生成必須
- **型安全性強制**: 全てのAPIでジェネリクス型パラメータ必須
- **Schema Registry必須**: スキーマレス運用不可、Registry連携前提
- **Fail-Fast原則**: スキーマ登録失敗時の即座終了

### パフォーマンス考慮事項
- **キャッシュ依存**: 初回生成コストが高い、キャッシュ効果への依存度大
- **メモリ使用量**: エンティティ別キャッシュによるメモリ消費
- **スキーマ生成コスト**: リフレクション多用による初期化時間
- **Schema Registry通信**: ネットワーク遅延・タイムアウト対策

### セキュリティ・品質制約
- **スキーマ整合性**: POCOとAvroスキーマの不整合検出
- **認証情報保護**: Schema Registry認証情報の適切な管理
- **バージョン互換性**: 後方互換性違反の防止
- **リソースリーク防止**: IDisposableの適切な実装

---

## 🔗 他Namespaceとの連携

### Core依存関係
```csharp
// EntityModel → AvroEntityConfiguration 変換
public AvroEntityConfiguration(Type entityType)
{
    EntityType = entityType;
    AutoConfigureFromAttributes(); // [Topic], [Key]等の属性自動読取
}

private void AutoConfigureFromAttributes()
{
    var topicAttribute = EntityType.GetCustomAttribute<TopicAttribute>();
    var keyProperties = Array.FindAll(allProperties, 
        p => p.GetCustomAttribute<KeyAttribute>() != null);
    
    TopicName = topicAttribute?.TopicName;
    KeyProperties = keyProperties;
}
```

### Application連携
```csharp
// スキーマ一括登録（Application層から呼び出し）
public async Task RegisterAllSchemasAsync(
    IReadOnlyDictionary<Type, AvroEntityConfiguration> configurations)
{
    foreach (var (entityType, config) in configurations)
    {
        await RegisterEntitySchemaAsync(entityType, config);
    }
}
```

### Messaging連携
```csharp
// Producer/Consumer用のSerializer提供
public async Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default)
{
    var entityModel = GetEntityModel<T>();
    return await _factory.CreateSerializersAsync<T>(entityModel, cancellationToken);
}

public async Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default)
{
    var entityModel = GetEntityModel<T>();
    return await _factory.CreateDeserializersAsync<T>(entityModel, cancellationToken);
}
```

### インターフェース定義
```csharp
// 型安全シリアライゼーション管理
public interface IAvroSerializationManager<T> : IDisposable where T : class
{
    Task<SerializerPair<T>> GetSerializersAsync(CancellationToken cancellationToken = default);
    Task<DeserializerPair<T>> GetDeserializersAsync(CancellationToken cancellationToken = default);
    Task<bool> ValidateRoundTripAsync(T entity, CancellationToken cancellationToken = default);
    SerializationStatistics GetStatistics();
    Type EntityType { get; }
}

// スキーマ生成プロバイダー
public interface IAvroSchemaProvider
{
    Task<string> GetKeySchemaAsync<T>() where T : class;
    Task<string> GetValueSchemaAsync<T>() where T : class;
    Task<(string keySchema, string valueSchema)> GetSchemasAsync<T>() where T : class;
}
```

### 協調動作パターン
1. **Application→Serialization**: 初期化時のスキーマ一括登録
2. **Serialization→Core**: EntityModel基準のスキーマ生成
3. **Messaging→Serialization**: Serializer/Deserializerペア取得
4. **Serialization→Configuration**: Schema Registry接続設定適用

---

## 💡 実装上の重要なポイント

### POCO属性駆動のスキーマ生成
```csharp
// 属性からのAvroフィールド生成
private object MapPropertyToAvroType(PropertyInfo property)
{
    var isNullable = IsNullableProperty(property);
    var avroType = GetAvroType(property);

    return isNullable ? new object[] { "null", avroType } : avroType;
}

private object GetAvroType(PropertyInfo property)
{
    // [DecimalPrecision]属性の特別処理
    if (underlyingType == typeof(decimal))
    {
        var decimalAttr = property.GetCustomAttribute<DecimalPrecisionAttribute>();
        return new
        {
            type = "bytes",
            logicalType = "decimal",
            precision = decimalAttr?.Precision ?? 18,
            scale = decimalAttr?.Scale ?? 4
        };
    }
    
    // [AvroTimestamp]属性の特別処理
    if (underlyingType == typeof(DateTime))
    {
        return new
        {
            type = "long",
            logicalType = "timestamp-millis"
        };
    }
}
```

### 型安全キャッシュ実装
```csharp
// エンティティ別シリアライザーキャッシュ
private readonly ConcurrentDictionary<Type, object> _serializerManagers = new();

public IAvroSerializationManager<T> GetAvroManager<T>() where T : class
{
    var entityType = typeof(T);

    if (_serializerManagers.TryGetValue(entityType, out var existingManager))
    {
        return (IAvroSerializationManager<T>)existingManager;
    }

    var newManager = new AvroEntitySerializationManager<T>(_factory, _logger);
    _serializerManagers[entityType] = newManager;
    return newManager;
}
```

### スキーマバージョニング実装
```csharp
// 互換性チェック付きアップグレード
public async Task<bool> CanUpgradeAsync<T>(string newSchema) where T : class
{
    var entityType = typeof(T);
    var topicName = GetTopicName(entityType);
    var subject = $"{topicName}-value";

    var schemaObj = new Schema(newSchema, SchemaType.Avro);
    return await _schemaRegistryClient.IsCompatibleAsync(subject, schemaObj);
}

public async Task<SchemaUpgradeResult> UpgradeAsync<T>() where T : class
{
    var newSchema = UnifiedSchemaGenerator.GenerateValueSchema<T>();
    var canUpgrade = await CanUpgradeAsync<T>(newSchema);
    
    if (!canUpgrade)
    {
        return new SchemaUpgradeResult
        {
            Success = false,
            Reason = "Schema is not compatible for upgrade"
        };
    }
    
    // アップグレード実行
    var newSchemaId = await _schemaRegistryClient.RegisterSchemaAsync(subject, schemaObj);
    return new SchemaUpgradeResult { Success = true, NewSchemaId = newSchemaId };
}
```

### Fail-Fast例外設計
```csharp
// 致命的スキーマ登録エラー
public class SchemaRegistrationFatalException : Exception
{
    public string Subject { get; }
    public int AttemptCount { get; }
    public SchemaRegistrationFailureCategory FailureCategory { get; }
    public string OperationalAction { get; }

    public string GetOperationalSummary()
    {
        return $@"
🚨 SCHEMA REGISTRATION FATAL ERROR 🚨
💀 HUMAN INTERVENTION REQUIRED - APPLICATION MUST BE TERMINATED 💀

📋 Error Details:
   Subject: {Subject}
   Failed At: {FailedAt:yyyy-MM-dd HH:mm:ss} UTC
   Category: {FailureCategory}

🔧 Required Action:
   {OperationalAction}
";
    }
}
```

### 統合スキーマ生成エンジン
```csharp
// 型からのトピックスキーマペア生成
public static (string keySchema, string valueSchema) GenerateTopicSchemas(
    AvroEntityConfiguration configuration)
{
    var keySchema = GenerateKeySchema(configuration);
    var valueSchema = GenerateValueSchema(configuration.EntityType, configuration);
    return (keySchema, valueSchema);
}

// 複合キー vs 単一キーの自動判定
public static string GenerateKeySchema(AvroEntityConfiguration configuration)
{
    if (!configuration.HasKeys())
        return GeneratePrimitiveKeySchema(typeof(string));

    if (configuration.KeyProperties!.Length == 1)
        return GenerateKeySchema(configuration.KeyProperties[0].PropertyType);

    // 複合キー
    return GenerateCompositeKeySchema(configuration.GetOrderedKeyProperties());
}
```

---

## 🎯 Serialization層の設計哲学

### 自動化ファースト
- **手動スキーマ定義なし**: POCO属性からの完全自動生成
- **型安全性**: ジェネリクスによるコンパイル時型チェック
- **キャッシュ最適化**: 初回生成後の高速アクセス

### 堅牢性重視
- **Fail-Fast**: スキーマ問題の早期発見・即座終了
- **バージョニング**: 互換性チェック付きスキーマ進化
- **リトライ機能**: 一時的障害に対する回復力

### 開発体験向上
- **属性駆動**: POCOクラスでの直感的設定
- **統計・監視**: パフォーマンス情報の可視化
- **デバッグ支援**: Round-trip検証による動作確認

Serialization層は、型安全性とパフォーマンスを両立しながら、POCO属性駆動による開発体験向上を実現する重要な基盤となっています。