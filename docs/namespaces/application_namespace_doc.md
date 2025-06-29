# Application 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **開発者向けAPI提供**：EF風の直感的なKafkaContext APIとFluent Builder設計
- **設定管理・バリデーション**：Schema Registry接続、ログ設定、初期化オプションの統合管理
- **スキーマ自動登録**：起動時のAvroスキーマ一括登録とFail-Fast設計
- **Producer/Consumer統合**：Pool削除による直接管理とシンプル化設計

### 設計原則・制約
- **EF風API**: DbContextライクな開発体験、親しみやすいインターフェース
- **Fail-Fast**: 初期化時エラーは即座に終了、実行時は適切な例外処理
- **自動化優先**: OnModelCreating → スキーマ登録 → 接続確認の自動フロー
- **Pool削除方針**: 複雑性削減のため、直接Manager使用でシンプル化

### 他namespaceとの境界
- **Core**: EntityModelとKafkaContextCoreを活用、型安全性を確保
- **Messaging**: ProducerManager/ConsumerManagerの直接管理
- **Serialization**: スキーマ登録サービスとの連携、Avro操作委譲
- **Query**: （直接連携なし、Core経由でEntitySet提供）

---

## 🏗️ 主要クラス構成

### 📁 Extensions/ - 拡張メソッド（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `AvroSchemaInfoExtensions.cs` | AvroSchemaInfoExtensions | スキーマSubject名生成、型推論ヘルパー | 🟢 |

### 📁 Context/ - コンテキスト実装（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KsqlContext.cs` | KafkaContext | 簡素化統合KafkaContext、Pool削除版 | 🔴 |
| `KsqlContext.cs` | EventSetWithServices<T> | 簡素化Manager統合EventSet | 🔴 |

### 📁 Builder/ - Fluent API（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KsqlContextBuilder.cs` | KsqlContextBuilder | Fluent API設定、コンテキスト構築 | 🟡 |

### 📁 Options/ - 設定管理（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KsqlContextOptions.cs` | KsqlContextOptions | 設定値管理、バリデーション | 🟡 |
| `KsqlContextOptionsExtensions.cs` | KsqlContextOptionsExtensions | 設定拡張メソッド | 🟡 |

---

## 🔄 データフロー・依存関係

### Context初期化フロー
```
KsqlContextBuilder.Create()
   ↓
UseSchemaRegistry() → KsqlContextOptions設定
   ↓
BuildContext<T>() → KafkaContext実装クラス生成
   ↓
OnModelCreating() → Core.ModelBuilder実行
   ↓
ConvertToAvroConfigurations() → AvroEntityConfiguration変換
   ↓
AvroSchemaRegistrationService → スキーマ一括登録（Fail-Fast）
```

### EntitySet操作フロー
```
context.Orders.AddAsync()
   ↓
EventSetWithServices<T>.AddAsync()
   ↓
KafkaProducerManager.SendAsync() → Messaging層委譲
   ↓
ProducerManager → SerializationManager → Kafka送信
```

### 設定適用フロー
```
KsqlContextOptions
   ↓
SchemaRegistryClient → ISchemaRegistryClient設定
LoggerFactory → ILoggerFactory設定
Validation設定 → AutoRegister/FailOnErrors等
   ↓
BuildContext時に各Manager初期化で使用
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. Context機能拡張**
- `KafkaContext`: 新しいEntitySet操作、Manager統合方式変更
 - `EventSetWithServices`: Producer/Consumer操作の拡張
- Core層との連携パターン改善

**2. 初期化・設定機能強化**
- OnModelCreating時の自動スキーマ登録ロジック改善
- ConvertToAvroConfigurations変換ロジック拡張
- Fail-Fast例外処理の精密化

**3. Pool削除対応の継続改善**
- Manager直接使用パターンの最適化
- リソース管理方式の改善
- エラーハンドリング強化

### 🟡 中頻度変更パターン
**1. Builder API拡張**
- `KsqlContextBuilder`: 新しい設定オプション対応
- Fluent API種別の追加
- 設定バリデーション強化

**2. Options機能拡張**
- `KsqlContextOptions`: 新しい設定項目追加
- Schema Registry以外の外部サービス設定
- 環境別設定プロファイル

**3. 設定拡張メソッド**
- `KsqlContextOptionsExtensions`: 便利メソッド追加
- 設定パターンの標準化

### 🟢 低頻度変更パターン
**1. Extensions拡張**
- `AvroSchemaInfoExtensions`: 新しいスキーマ操作
- Subject命名規則変更（慎重に実施）

**2. アーキテクチャ変更**
- Pool戦略の見直し（現在は直接管理）
- Context初期化フローの根本的変更

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **Pool削除方針**: ProducerPool/ConsumerPoolは使用しない、Manager直接管理
- **Fail-Fast原則**: スキーマ登録失敗時は即座にアプリケーション終了
- **EF風API維持**: DbContextライクなインターフェース設計の堅持
- **自動化優先**: OnModelCreating完了後の自動スキーマ登録強制

### パフォーマンス考慮事項
- **初期化時間**: スキーマ一括登録によるスタートアップ時間への影響
- **Manager直接管理**: Pool削除によるシンプル化とリソース効率化
- **設定バリデーション**: 起動時の設定検証コスト
- **リソースリーク防止**: IDisposableの適切な実装

### セキュリティ・品質制約
- **Schema Registry認証**: SSL/SASL設定の適切な管理
- **設定検証**: 不正設定の早期検出
- **例外透過性**: Core/Infrastructure例外の適切な伝播
- **ログ出力**: 初期化・設定関連の詳細ログ

---

## 🔗 他Namespaceとの連携

### Core依存関係
```csharp
// EntityModel: POCO→Avro設定変換
protected IReadOnlyDictionary<Type, AvroEntityConfiguration> ConvertToAvroConfigurations(
    Dictionary<Type, EntityModel> entityModels)
{
    foreach (var kvp in entityModels)
    {
        var entityModel = kvp.Value;
        var avroConfig = new AvroEntityConfiguration(entityModel.EntityType)
        {
            TopicName = entityModel.TopicAttribute?.TopicName,
            KeyProperties = entityModel.KeyProperties
        };
        avroConfigs[kvp.Key] = avroConfig;
    }
}
```

### Messaging連携
```csharp
// 簡素化Manager（Pool削除版）
public KafkaContext()
{
    _producerManager = new KafkaProducerManager(
        Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
        null   // LoggerFactory
    );

    _consumerManager = new KafkaConsumerManager(
        Microsoft.Extensions.Options.Options.Create(new KsqlDslOptions()),
        null   // LoggerFactory
    );
}

// EventSet送信処理
protected override async Task SendEntityAsync(T entity, CancellationToken cancellationToken)
{
    var producerManager = _kafkaContext.GetProducerManager();
    await producerManager.SendAsync(entity, cancellationToken);
}
```

### Serialization連携
```csharp
// Schema Registry設定
public KsqlContextBuilder UseSchemaRegistry(string url)
{
    _options.UseSchemaRegistry(url);
    return this;
}

public KsqlContextOptions UseSchemaRegistry(this KsqlContextOptions options, string url)
{
    var config = new SchemaRegistryConfig { Url = url };
    options.SchemaRegistryClient = new CachedSchemaRegistryClient(config);
    return options;
}
```

### インターフェース定義
```csharp
// Builder API
KsqlContextBuilder UseSchemaRegistry(string url);
KsqlContextBuilder EnableLogging(ILoggerFactory loggerFactory);
KsqlContextBuilder ConfigureValidation(bool autoRegister, bool failOnErrors, bool enablePreWarming);
T BuildContext<T>() where T : KafkaContext;

// Options API
void Validate();
ISchemaRegistryClient SchemaRegistryClient { get; set; }
bool AutoRegisterSchemas { get; set; }
bool FailOnInitializationErrors { get; set; }
```

### 協調動作パターン
1. **Builder→Options**: Fluent APIでの設定値設定
2. **Options→Context**: 初期化時の設定適用
3. **Context→Core**: OnModelCreating、EntitySet管理
4. **Context→Messaging**: Manager直接使用、Pool回避
5. **Context→Serialization**: スキーマ登録サービス連携

---

## 💡 実装上の重要なポイント

### Pool削除による直接Manager使用
```csharp
// ✅ 新設計: 直接Manager使用
internal KafkaProducerManager GetProducerManager() => _producerManager;
internal KafkaConsumerManager GetConsumerManager() => _consumerManager;

// ❌ 旧設計: Pool使用（削除済み）
// private readonly ProducerPool _producerPool;
// private readonly ConsumerPool _consumerPool;
```

### Fail-Fast設計の実装
```csharp
public void Validate()
{
    if (SchemaRegistryClient == null)
        throw new InvalidOperationException("SchemaRegistryClient is required");

    if (SchemaRegistrationTimeout <= TimeSpan.Zero)
        throw new InvalidOperationException("SchemaRegistrationTimeout must be positive");
}
```

### EF風API設計
```csharp
// DbContextライクなAPI
protected override IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel)
{
    return new EventSetWithServices<T>(this, entityModel);
}

// DbSet<T>相当のEntitySet<T>
public IEntitySet<Order> Orders => Set<Order>();
```

### ConvertToAvroConfigurations実装
```csharp
protected IReadOnlyDictionary<Type, AvroEntityConfiguration> ConvertToAvroConfigurations(
    Dictionary<Type, EntityModel> entityModels)
{
    var avroConfigs = new Dictionary<Type, AvroEntityConfiguration>();

    foreach (var kvp in entityModels)
    {
        var entityModel = kvp.Value;
        var avroConfig = new AvroEntityConfiguration(entityModel.EntityType)
        {
            TopicName = entityModel.TopicAttribute?.TopicName,
            KeyProperties = entityModel.KeyProperties
        };

        avroConfigs[kvp.Key] = avroConfig;
    }

    return avroConfigs;
}
```

### Builder実装パターン
```csharp
public T BuildContext<T>() where T : KafkaContext
{
    var options = Build(); // 設定検証実行

    // KsqlContextOptions引数のコンストラクタ優先
    var ctor = typeof(T).GetConstructor(new[] { typeof(KsqlContextOptions) });
    if (ctor != null)
    {
        return (T)ctor.Invoke(new object[] { options });
    }

    // パラメータレスコンストラクタにフォールバック
    ctor = typeof(T).GetConstructor(Type.EmptyTypes);
    if (ctor != null)
    {
        return (T)ctor.Invoke(null);
    }

    // 最後の手段: Activator使用
    return (T)Activator.CreateInstance(typeof(T), options)!;
}
```

---

## 🎯 Application層の設計哲学

### 開発体験重視
- **親しみやすさ**: EF CoreのDbContextに似た API設計
- **自動化**: 面倒な初期化処理の自動化
- **Fail-Fast**: 設定ミスの早期発見

### シンプル化原則
- **Pool削除**: 複雑性排除、直接Manager使用
- **設定統合**: 散在していた設定の一元管理
- **Builder統合**: 設定とContext生成の統合API

### 堅牢性確保
- **設定バリデーション**: 実行前の設定チェック
- **例外伝播**: 下位層例外の適切な伝播
- **リソース管理**: 適切なDispose実装

Application層は開発者が最初に触れる部分として、使いやすさと堅牢性を両立した設計となっています。