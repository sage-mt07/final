# Query 詳細設計

## 🎯 責務・設計方針

### 主要責務
- **LINQ式からKSQL文への変換**：C# LINQ式を解析してksqlDB互換のKSQL文を生成
- **クエリ実行パイプライン管理**：段階的なクエリ実行（派生オブジェクト作成→最終実行）
- **DDL/DML分離生成**：CREATE文（DDL）とSELECT文（DML）の適切な使い分け
- **Stream/Table型推論**：LINQ操作に基づく最適なksqlDBオブジェクト型の決定

### 設計原則・制約
- **型安全性確保**：LINQ式の型情報を保持してKSQL生成時のエラーを防止
- **表現力の限界**：OrderByなど未サポート操作は明示的な例外で即座に通知
- **パフォーマンス重視**：派生オブジェクト戦略により複雑なクエリを段階的に最適化
- **診断可能性**：生成されるKSQL文をToKsql()で事前確認可能

### 他namespaceとの境界
- **Core**: EntityModelを受け取り、型情報に基づく変換を実行
- **Messaging**: 実際のKafka通信は委譲、Query層は純粋に変換処理のみ
- **Serialization**: スキーマ情報の取得のみ、直接的なシリアライゼーション処理なし

---

## 🏗️ 主要クラス構成

### 📁 Pipeline/ - 実行制御コア（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `QueryExecutionPipeline.cs` | QueryExecutionPipeline | LINQ式解析→派生オブジェクト作成→クエリ実行の統括制御 | 🔴 |
| `DDLQueryGenerator.cs` | DDLQueryGenerator | CREATE STREAM/TABLE AS文の生成 | 🔴 |
| `DMLQueryGenerator.cs` | DMLQueryGenerator | SELECT文の生成 | 🟡 |
| `StreamTableAnalyzer.cs` | StreamTableAnalyzer | 操作チェーンからStream/Table型推論 | 🔴 |
| `DerivedObjectManager.cs` | DerivedObjectManager | 中間オブジェクト（stream_123等）の管理 | 🟡 |

### 📁 Builders/ - KSQL構文生成（🔴高変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `SelectBuilder.cs` | SelectBuilder | WHERE句構築（条件式の変換） | 🔴 |
| `ProjectionBuilder.cs` | ProjectionBuilder | SELECT列の構築（射影・AS句含む） | 🔴 |
| `GroupByBuilder.cs` | GroupByBuilder | GROUP BY句構築 | 🟡 |
| `JoinBuilder.cs` | JoinBuilder | JOIN句構築（複数テーブル結合） | 🟡 |
| `HavingBuilder.cs` | HavingBuilder | HAVING句構築（集約後条件） | 🟢 |
| `WindowBuilder.cs` | WindowBuilder | WINDOW句構築（時間ウィンドウ） | 🟢 |

### 📁 Ksql/ - ksqlDB REST API通信（🟡中変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `KsqlDbRestApiClient.cs` | KsqlDbRestApiClient | ksqlDB REST API通信、ストリーミング対応 | 🟡 |
| `KsqlDbExecutor.cs` | KsqlDbExecutor | DDL/DMLクエリ実行の抽象化 | 🟡 |

### 📁 Linq/ - JOIN拡張（🟢低変更頻度）
| ファイル | クラス | 責務 | 変更頻度 |
|---------|--------|------|----------|
| `UnifiedJoinResult.cs` | UnifiedJoinResult<TOuter,TInner> | 2テーブルJOIN結果 | 🟢 |
| `UnifiedThreeWayJoinResult.cs` | UnifiedThreeWayJoinResult<TOuter,TInner,TThird> | 3テーブルJOIN結果 | 🟢 |
| `JoinableEntitySet.cs` | JoinableEntitySet<T> | JOIN可能なEntitySet | 🟢 |

---

## 🔄 データフロー・依存関係

### 主要処理フロー
```
LINQ式 → StreamTableAnalyzer → QueryExecutionPipeline
   ↓
ExpressionAnalysisResult → DerivedObjectManager
   ↓
必要に応じて派生オブジェクト作成（DDLQueryGenerator）
   ↓
最終SELECT実行（DMLQueryGenerator）
   ↓
ksqlDB REST API（KsqlDbRestApiClient）
```

### Builder使用パターン
```
Expression → 各Builder（WHERE/SELECT/GROUP BY等）→ KSQL文字列断片
                            ↓
                    DDL/DMLQueryGeneratorで組み立て
                            ↓
                      完全なKSQL文
```

### 型推論ロジック
```
MethodCall解析 → 集約有無判定 → Stream/Table決定
- Where/Select → Stream
- GroupBy → Table
- Select after GroupBy → Table
```

---

## 🚀 変更頻度・作業パターン

### 🔴 高頻度変更パターン
**1. 新しいLINQ演算子対応**
- `StreamTableAnalyzer`に操作特性追加
- 対応する`Builder`クラス作成または拡張
- `QueryExecutionPipeline`での処理分岐追加

**2. KSQL構文の改善**
- `SelectBuilder`でのWHERE条件表現拡張（null比較、複合条件等）
- `ProjectionBuilder`でのSELECT列表現拡張
- 新しいKSQL関数の対応

**3. エラーハンドリング強化**
- 未サポート操作の早期検出
- より詳細なエラーメッセージ
- デバッグ情報の充実

### 🟡 中頻度変更パターン
**1. JOIN機能拡張**
- 新しいJOIN種別（LEFT/RIGHT/FULL OUTER）
- より複雑なJOIN条件
- サブクエリ含むJOIN

**2. ksqlDB APIアップデート対応**
- 新しいREST APIエンドポイント
- レスポンス形式の変更対応
- 認証・セキュリティ拡張

### 🟢 低頻度変更パターン
**1. ウィンドウ機能拡張**
- 新しいウィンドウタイプ
- Grace Period等のオプション拡張

**2. パフォーマンス最適化**
- クエリ実行プラン改善
- キャッシュ機能追加

---

## 📝 設計制約・注意事項

### アーキテクチャ制約
- **OrderBy未サポート**：ksqlDBのストリーミング性質上、ORDER BYは原則サポート外
- **サブクエリ制限**：ネストしたLINQ式は段階的な派生オブジェクトで解決
- **型安全性保持**：Expressionツリーの型情報を最後まで保持する必要性

### パフォーマンス考慮事項
- **派生オブジェクトのライフサイクル**：不要になった中間オブジェクトの適切なクリーンアップ
- **ksqlDB接続プール**：REST API通信のコネクション管理
- **大量データ処理**：Pull QueryとPush Queryの適切な使い分け

### セキュリティ・品質制約
- **SQLインジェクション対策**：動的文字列結合ではなくParameterized Query相当の実装
- **スキーマ整合性**：EntityModelとksqlDBスキーマの不整合検出
- **リソースリーク防止**：IDisposableの適切な実装（特にDerivedObjectManager）

---

## 🔗 他Namespaceとの連携

### Core依存関係
- **EntityModel**: エンティティのメタデータ（属性、キープロパティ等）取得
- **IKafkaContext**: コンテキスト情報とEntitySet管理
- **各種Attribute**: Topic名、Key指定等の属性情報活用

### Messaging連携
- **KafkaConsumerManager**: Pull/Push Query実行時のConsumer取得
- **KafkaProducerManager**: （直接使用なし、Contextレイヤー経由）

### StateStore連携（間接的）
- **派生オブジェクト**: KTableとして作成されたオブジェクトはStateStoreで管理される可能性

### インターフェース定義
```csharp
// 主要な外部インターフェース
IQueryTranslator.ToKsql(Expression, string, bool) → string
IDDLQueryGenerator.GenerateCreateStreamAs(string, string, Expression) → string  
IDMLQueryGenerator.GenerateSelectWithCondition(string, Expression, bool) → string
```

### 協調動作パターン
1. **EventSet→Query連携**: EventSet.ToKsql()からの変換要求
2. **Query→Messaging連携**: 生成KSQLのksqlDB実行
3. **Query→StateStore連携**: CREATE TABLEとして生成されたオブジェクトの管理

---

## 💡 実装上の重要なポイント

### Expression解析のベストプラクティス
- **Visitor Pattern活用**: 各BuilderでExpressionVisitorを継承
- **型変換対応**: UnaryExpressionでのConvert操作の適切な処理
- **null安全性**: ConstantExpressionでのnull値の明示的ハンドリング

### エラー処理方針
- **早期失敗**: 未サポート操作はNotSupportedExceptionで即座に停止
- **診断情報**: 問題のあるExpression箇所を特定可能なメッセージ
- **フォールバック**: 部分的な変換失敗時の適切なデフォルト動作

### テスト戦略
- **Expression解析テスト**: 各種LINQ式パターンの網羅的テスト
- **KSQL生成テスト**: 期待されるKSQL文との文字列比較
- **統合テスト**: 実際のksqlDBとの連携動作確認