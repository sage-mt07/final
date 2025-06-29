# Core 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **ビジネスロジック抽象化**: Infrastructure層への依存を排除した純粋なドメインロジック
- **型安全性・依存関係制御**: IKafkaContext、IEntitySet等の抽象インターフェース定義
- **エンティティモデル構築**: POCO属性ベースのEntityModel生成とFluent API
- **POCO属性システム**: `[Topic]`, `[Key]`, `[KafkaIgnore]`等の属性定義と処理

### 設計原則・制約
- **依存制限**: Infrastructure層（Messaging, Serialization等）への依存禁止
- **純粋関数**: 副作用なし、テスタブルな設計
- **型安全性**: ジェネリクス活用による型安全なAPI設計
- **POCO属性主導**: 手動設定より属性ベース自動構成を優先

### 他namespaceとの境界
- **Application**: KafkaContextCore基底クラスとEntityModel提供
- **Infrastructure**: 抽象インターフェース定義のみ、具象実装は下位層
- **依存方向**: Core ← Infrastructure（一方向依存を強制）

---

## 🏗️ 主要クラス構成

### 📁 Abstractions/ - 抽象化定義（🟢低変更頻度）
| ファイル | クラス/インターフェース | 責務 | 変更頻度 |
|---------|----------------------|------|----------|
| `IKafkaContext.cs` | IKafkaContext | コンテキスト操作の抽象化 | 🟢 |
| `IEntitySet.cs` | IEntitySet<T> | CRUD操作の統一API | 🟢 |
| `ISerializationManager.cs` | ISerializationManager<T> | シリアライゼーション抽象化 | 🟢 |
| `EntityModel.cs` | EntityModel | エンティティメタデータ管理 | 🟡 |
| `TopicAttribute.cs` | TopicAttribute | トピック属性定義 | 🟢 |
| `KeyAttribute.cs` | KeyAttribute | キー属性定義 | 🟢 |
| `KafkaIgnoreAttribute.cs` | KafkaIgnoreAttribute | 除外属性定義 | 🟢 |
| `AvroTimestampAttribute.cs` | AvroTimestampAttribute | タイムスタンプ属性定義 | 🟢 |
| `DecimalPrecisionAttribute.cs` | DecimalPrecisionAttribute | Decimal精度属性定義 | 🟢 |

### 📁 Context/ - コンテキスト基底（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KafkaContextCore.cs` | KafkaContextCore | KafkaContext基底実装 | 🟡 |
| `KafkaContextOptions.cs` | KafkaContextOptions | Core層設定オプション | 🟡 |

### 📁 Modeling/ - モデル構築（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `ModelBuilder.cs` | ModelBuilder | Fluent API + 属性ベース設定 | 🔴 |
| `EntityModelBuilder.cs` | EntityModelBuilder<T> | 個別エンティティ設定 | 🔴 |
| `ModelValidationResult.cs` | ModelValidationResult | モデル検証結果 | 🟡 |
| `WindowModelBuilderExtensions.cs` | Window拡張 | ウィンドウ集約設定 | 🟡 |

### 📁 Extensions/ - 拡張メソッド（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `CoreExtensions.cs` | CoreExtensions | EntityModel拡張メソッド | 🟡 |
| `EntityModelWindowExtensions.cs` | Window拡張 | Window関連拡張 | 🟡 |
| `LoggerFactoryExtensions.cs` | ログ拡張 | 汎用ログ拡張メソッド | 🟢 |
| `WindowExtensions.cs` | WindowExtensions | `.Window(minutes)`拡張 | 🟡 |

### 📁 Models/ - データモデル（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KeyExtractor.cs` | KeyExtractor | エンティティキー抽出ロジック | 🟡 |
| `ProducerKey.cs` | ProducerKey | Producer識別キー | 🟡 |

### 📁 Window/ - ウィンドウ処理（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `WindowedEntitySet.cs` | WindowedEntitySet<T> | ウィンドウ付きEntitySet | 🟡 |
| `WindowCollection.cs` | WindowCollection<T> | 複数ウィンドウ管理 | 🟡 |
| `WindowAggregatedEntitySet.cs` | WindowAggregatedEntitySet | ウィンドウ集約EntitySet | 🔴 |

### 📁 Abstractions/ErrorHandling/ - エラー処理（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `ErrorHandlingPolicy.cs` | ErrorHandlingPolicy | エラー処理ポリシー定義 | 🟡 |
| `ErrorHandlingExtensions.cs` | 拡張メソッド | エラー処理拡張API | 🟡 |
| `CircuitBreakerHandler.cs` | CircuitBreakerHandler | 回路ブレーカー実装 | 🟡 |

### 📁 Exceptions/ - 例外定義（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `CoreException.cs` | CoreException | Core例外基底クラス | 🟢 |
| `EntityModelException.cs` | EntityModelException | エンティティモデル例外 | 🟢 |
| `CoreValidationException.cs` | CoreValidationException | 検証例外 | 🟢 |

---

## 🔄 データフロー・依存関係

### EntityModel構築フロー
```
POCO型定義 → ModelBuilder.Entity<T>()
   ↓
CreateEntityModelFromType() → 属性読取り
   ↓
TopicAttribute, KeyAttribute等の解析
   ↓
EntityModel生成 → ValidationResult付与
   ↓
EntityModelBuilder<T>によるFluent API設定
```

### Context初期化フロー
```
KafkaContextCore.OnModelCreating()
   ↓
ModelBuilder作成 → Entity登録
   ↓
ApplyModelBuilderSettings() → Stream/Table型設定
   ↓
GetOrCreateEntityModel() → EntityModel取得・キャッシュ
   ↓
CreateEntitySet<T>() → 派生クラスでの具象実装
```

### EntitySet操作フロー
```
IKafkaContext.Set<T>()
   ↓
GetOrCreateEntityModel<T>() → EntityModel取得
   ↓
CreateEntitySet<T>(entityModel) → 抽象メソッド
   ↓
派生クラスで具象EventSet<T>生成
```

### Window操作フロー
```
EntitySet.Window(minutes) → WindowedEntitySet<T>
   ↓
ValidateAndGetTimestampProperty() → [AvroTimestamp]検証
   ↓
GroupByAggregate() → WindowAggregatedEntitySet生成
   ↓
KSQL Window句生成 → Infrastructure層委譲
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. ModelBuilder機能拡張**
- `ModelBuilder`: 新しいFluent API機能、属性解析ロジック拡張
- `EntityModelBuilder`: 個別エンティティ設定オプション追加
- POCO属性の新機能サポート（`[KafkaIgnore]`, `[AvroTimestamp]`等）

**2. Window処理機能拡張**
- `WindowAggregatedEntitySet`: 新しい集約関数、ウィンドウ種別対応
- Window操作のKSQL生成ロジック改善
- ウィンドウ集約の式木解析強化

**3. EntityModel機能強化**
- Stream/Table型推論ロジック改善
- 複合キー処理の拡張
- バリデーション機能強化

### 🟡 中頻度変更パターン
**1. 拡張メソッド追加**
- `CoreExtensions`: EntityModel操作の便利メソッド
- `WindowExtensions`: ウィンドウ操作の新機能
- エラーハンドリング拡張の改善

**2. コンテキスト基底機能**
- `KafkaContextCore`: 基底機能の改善、パフォーマンス最適化
- リソース管理（IDisposable/IAsyncDisposable）の強化

**3. データモデル拡張**
- `KeyExtractor`: 新しいキー抽出パターン
- エンティティメタデータ管理の改善

### 🟢 低頻度変更パターン
**1. 抽象インターフェース変更**
- `IKafkaContext`, `IEntitySet<T>`: 新機能追加（破壊的変更は慎重）
- 基本属性定義の変更

**2. 例外体系の変更**
- 新しい例外種別追加
- エラー分類の見直し

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **Infrastructure層依存禁止**: Messaging, Serialization等への直接依存不可
- **純粋関数原則**: 副作用なし、テスタブルな設計必須
- **一方向依存**: Core ← Infrastructure の依存方向強制
- **型安全性強制**: 全てのAPIでジェネリクス型パラメータ必須

### パフォーマンス考慮事項
- **リフレクション最適化**: 属性読取り・型解析のキャッシュ化
- **EntityModel生成コスト**: 初期化時の属性解析オーバーヘッド
- **Window操作メモリ**: 集約処理時のメモリ使用量制御
- **式木解析コスト**: LINQ式の解析・変換パフォーマンス

### セキュリティ・品質制約
- **型安全性保証**: 実行時型エラーの防止
- **バリデーション強制**: EntityModelの整合性チェック
- **例外設計**: 適切な例外階層と情報提供
- **リソース管理**: 適切なDispose実装

---

## 🔗 他Namespaceとの連携

### Application連携
```csharp
// KafkaContextCoreの基底機能提供
public abstract class KafkaContextCore : IKafkaContext
{
    private readonly Dictionary<Type, EntityModel> _entityModels = new();
    
    // 派生クラスでの実装必須
    protected abstract IEntitySet<T> CreateEntitySet<T>(EntityModel entityModel) where T : class;
    
    // EntityModel管理
    public Dictionary<Type, EntityModel> GetEntityModels() => new(_entityModels);
}
```

### Infrastructure連携（抽象化のみ）
```csharp
// Infrastructure層への抽象インターフェース提供
public interface IKafkaContext : IDisposable, IAsyncDisposable
{
    IEntitySet<T> Set<T>() where T : class;
    object GetEventSet(Type entityType);
    Dictionary<Type, EntityModel> GetEntityModels();
}

public interface IEntitySet<T> : IAsyncEnumerable<T> where T : class
{
    Task AddAsync(T entity, CancellationToken cancellationToken = default);
    Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);
    Task ForEachAsync(Func<T, Task> action, TimeSpan timeout = default, CancellationToken cancellationToken = default);
    string GetTopicName();
    EntityModel GetEntityModel();
    IKafkaContext GetContext();
}
```

### モデル構築連携
```csharp
// POCO属性からEntityModel生成
private EntityModel CreateEntityModelFromType(Type entityType)
{
    var topicAttribute = entityType.GetCustomAttribute<TopicAttribute>();
    var allProperties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
    var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);

    var model = new EntityModel
    {
        EntityType = entityType,
        TopicAttribute = topicAttribute,
        AllProperties = allProperties,
        KeyProperties = keyProperties
    };

    // 検証実行
    model.ValidationResult = ValidateEntityModel(entityType, model);
    return model;
}
```

### インターフェース定義
```csharp
// Fluent API Builder
public interface IModelBuilder
{
    IEntityBuilder<T> Entity<T>() where T : class;
}

public interface IEntityBuilder<T> where T : class
{
    IEntityBuilder<T> AsTable(string? topicName = null, bool useCache = true);
    IEntityBuilder<T> AsStream();
    IEntityBuilder<T> WithManualCommit();
}

// ウィンドウ操作
public interface IWindowedEntitySet<T> : IEntitySet<T> where T : class
{
    int WindowMinutes { get; }
    IEntitySet<TResult> GroupByAggregate<TKey, TResult>(
        Expression<Func<T, TKey>> groupByExpression,
        Expression<Func<IGrouping<TKey, T>, TResult>> aggregationExpression,
        TimeSpan? gracePeriod = null) where TResult : class;
}
```

### 協調動作パターン
1. **Core→Application**: KafkaContextCore基底クラス提供
2. **Application→Core**: OnModelCreating、EntitySet管理
3. **Infrastructure→Core**: 抽象インターフェース実装
4. **Core→Infrastructure**: EntityModel、設定情報提供

---

## 💡 実装上の重要なポイント

### POCO属性駆動のEntityModel生成
```csharp
// 属性ベース自動設定
private void AutoConfigureFromAttributes()
{
    // Topic属性の確認
    var topicAttribute = EntityType.GetCustomAttribute<TopicAttribute>();
    if (topicAttribute != null)
    {
        TopicName = topicAttribute.TopicName;
        if (topicAttribute.PartitionCount > 0)
            Partitions = topicAttribute.PartitionCount;
    }

    // Key属性の確認・ソート
    var keyProperties = Array.FindAll(allProperties, p => p.GetCustomAttribute<KeyAttribute>() != null);
    Array.Sort(keyProperties, (p1, p2) =>
    {
        var order1 = p1.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
        var order2 = p2.GetCustomAttribute<KeyAttribute>()?.Order ?? 0;
        return order1.CompareTo(order2);
    });
    KeyProperties = keyProperties;
}
```

### 型安全なEntitySet管理
```csharp
// ジェネリクス型安全性確保
public IEntitySet<T> Set<T>() where T : class
{
    var entityType = typeof(T);

    if (_entitySets.TryGetValue(entityType, out var existingSet))
    {
        return (IEntitySet<T>)existingSet;
    }

    var entityModel = GetOrCreateEntityModel<T>();
    var entitySet = CreateEntitySet<T>(entityModel);
    _entitySets[entityType] = entitySet;

    return entitySet;
}
```

### Window操作の式木解析
```csharp
// ウィンドウ集約のExpression解析
internal class GroupByExpressionVisitor : ExpressionVisitor
{
    private readonly List<string> _columns = new();

    public List<string> GetColumns() => _columns;

    protected override Expression VisitMember(MemberExpression node)
    {
        _columns.Add(node.Member.Name.ToUpper());
        return base.VisitMember(node);
    }

    protected override Expression VisitNew(NewExpression node)
    {
        foreach (var arg in node.Arguments)
        {
            if (arg is MemberExpression member)
            {
                _columns.Add(member.Member.Name.ToUpper());
            }
        }
        return base.VisitNew(node);
    }
}
```

### エラーハンドリングポリシー
```csharp
// 高度なエラーハンドリング設定
public static ErrorHandlingPolicy ExponentialBackoff(int maxRetries = 3, TimeSpan baseInterval = default)
{
    var interval = baseInterval == default ? TimeSpan.FromSeconds(1) : baseInterval;

    return new ErrorHandlingPolicy
    {
        Action = ErrorAction.Retry,
        RetryCount = maxRetries,
        DynamicRetryInterval = attempt => TimeSpan.FromMilliseconds(
            interval.TotalMilliseconds * Math.Pow(2, attempt - 1))
    };
}

public static ErrorHandlingPolicy CircuitBreaker(int failureThreshold = 5, TimeSpan recoveryInterval = default)
{
    var recovery = recoveryInterval == default ? TimeSpan.FromMinutes(1) : recoveryInterval;

    return new ErrorHandlingPolicy
    {
        Action = ErrorAction.Skip,
        CustomHandler = new CircuitBreakerHandler(failureThreshold, recovery).Handle
    };
}
```

### バリデーション・例外設計
```csharp
// EntityModel検証
private ValidationResult ValidateEntityModel(Type entityType, EntityModel model)
{
    var result = new ValidationResult { IsValid = true };

    // エンティティ型の基本検証
    if (!entityType.IsClass || entityType.IsAbstract)
    {
        result.IsValid = false;
        result.Errors.Add($"Entity type {entityType.Name} must be a concrete class");
    }

    // Topic属性の検証
    if (model.TopicAttribute == null)
    {
        if (_validationMode == ValidationMode.Strict)
        {
            result.IsValid = false;
            result.Errors.Add($"Entity {entityType.Name} must have [Topic] attribute");
        }
        else
        {
            result.Warnings.Add($"Entity {entityType.Name} does not have [Topic] attribute, using class name as topic");
        }
    }

    return result;
}
```

### 依存関係検証
```csharp
// Core層の依存関係制約チェック
public static ValidationResult ValidateCoreDependencies()
{
    var result = new ValidationResult { IsValid = true };
    var coreAssembly = typeof(IKafkaContext).Assembly;
    var coreTypes = coreAssembly.GetTypes()
        .Where(t => t.Namespace?.StartsWith("Kafka.Ksql.Linq.Core") == true);

    foreach (var type in coreTypes)
    {
        ValidateTypedependencies(type, result);
    }

    return result;
}

private static void ValidateTypedependencies(Type type, ValidationResult result)
{
    var dependencies = type.GetReferencedTypes();

    foreach (var dependency in dependencies)
    {
        if (dependency.Namespace != null &&
            ForbiddenNamespaces.Any(ns => dependency.Namespace.StartsWith(ns)))
        {
            result.IsValid = false;
            result.Errors.Add(
                $"Core type {type.Name} has forbidden dependency on {dependency.Namespace}.{dependency.Name}");
        }
    }
}
```

---

## 🎯 Core層の設計哲学

### 純粋性重視
- **副作用なし**: 純粋関数による予測可能な動作
- **テスタブル**: Infrastructure依存なしの単体テスト可能性
- **依存制御**: 抽象インターフェースによる疎結合

### 型安全性確保
- **コンパイル時チェック**: ジェネリクスによる型安全性
- **実行時検証**: EntityModelバリデーションによる整合性確保
- **例外設計**: 適切な例外階層による問題の早期発見

### 拡張性・保守性
- **POCO属性駆動**: 直感的な設定方式
- **Fluent API**: 柔軟な設定オプション
- **モジュラー設計**: 機能別の明確な責務分離

Core層は、型安全性と純粋性を維持しながら、POCO属性駆動による直感的な開発体験を提供する、アーキテクチャの中核となる重要な層です。