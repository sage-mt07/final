# Kafka.Ksql.Linq

 Kafka／ksqlDB向けEntity Framework風DSLライブラリ

## 特徴
Kafka.Ksql.Linqは、Kafka／ksqlDB向けのクエリを  

C#のLINQスタイルで簡潔かつ直感的に記述できる、Entity Framework風のDSLライブラリです。  

既存のRDB開発経験者でも、Kafkaストリーム処理やKSQL文の記述・運用を  

.NETの慣れ親しんだ形で実現できることを目指しています。

## サンプルコード

```
using Kafka.Ksql.Linq.Application;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

[Topic("manual-commit-orders")]
public class ManualCommitOrder
{
    [Key]
    public int OrderId { get; set; }
    public decimal Amount { get; set; }
}

public class ManualCommitContext : KsqlContext
{
    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ManualCommitOrder>()
            .WithManualCommit();
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

        var context = KsqlContextBuilder.Create()
            .UseConfiguration(configuration)
            .UseSchemaRegistry(configuration["KsqlDsl:SchemaRegistry:Url"]!)
            .EnableLogging(LoggerFactory.Create(builder => builder.AddConsole()))
            .BuildContext<ManualCommitContext>();

        var order = new ManualCommitOrder
        {
            OrderId = Random.Shared.Next(),
            Amount = 10m
        };

        await context.Set<ManualCommitOrder>().AddAsync(order);
        await Task.Delay(500);

        await context.Set<ManualCommitOrder>().ForEachAsync(async (IManualCommitMessage<ManualCommitOrder> msg) =>
        {
            try
            {
                Console.WriteLine($"Processing order {msg.Value.OrderId}: {msg.Value.Amount}");
                await msg.CommitAsync();
            }
            catch
            {
                await msg.NegativeAckAsync();
            }
        });
    }
}

```


## Quick Start
### 1. インストール
### 2. 設定
### 3. 使用例
###📂  4. サンプルコード

実行可能なサンプルは `examples/` フォルダーにまとまっています。Producer と Consumer をペアで収録しており、各READMEに手順を記載しています。

- [hello-world](./examples/hello-world/) - 最小構成のメッセージ送受信
- [basic-produce-consume](./examples/basic-produce-consume/) - getting-started の基本操作
- [window-finalization](./examples/window-finalization/) - タンブリングウィンドウ集計の確定処理
- [error-handling](./examples/error-handling/) - リトライとエラーハンドリングの基礎
- [error-handling-dlq](./examples/error-handling-dlq/) - DLQ運用を含むエラー処理
- [configuration](./examples/configuration/) - 環境別のログ設定例
- [configuration-mapping](./examples/configuration-mapping/) - appsettings と DSL 設定のマッピング
- [manual-commit](./examples/manual-commit/) - 手動コミットの利用例
- [sqlserver-vs-kafka](./examples/sqlserver-vs-kafka/) - SQL Server 操作との対比
- [api-showcase](./examples/api-showcase/) - 代表的な DSL API の利用例


## 📚 ドキュメント構成ガイド

このOSSでは、利用者のレベルや目的に応じて複数のドキュメントを用意しています。

### 🧑‍🏫 初級〜中級者向け（Kafkaに不慣れな方）
| ドキュメント | 内容概要 |
|--|--|
| `docs/sqlserver-to-kafka-guide.md` | [SQL Server経験者向け：Kafkaベースの開発導入ガイド](./docs/sqlserver-to-kafka-guide.md) |
| `docs/getting-started.md` | [はじめての方向け：基本構成と動作確認手順](./docs/getting-started.md) |

### 🛠️ 上級開発者向け（DSL実装や拡張が目的の方）
| ドキュメント | 内容概要 |
|--|--|
| `docs/dev_guide.md` | [OSSへの機能追加・実装フローと開発ルール](./docs/dev_guide.md) |
| `docs/namespaces/*.md` | 各Namespace（Core / Messaging 等）の役割と構造 |

### 🏗️ アーキテクト・運用担当者向け（構造や制約を把握したい方）
| ドキュメント | 内容概要 |
|--|--|
| `docs/docs_advanced_rules.md` | [運用設計上の制約、設計判断の背景と意図](./docs/docs_advanced_rules.md) |
| `docs/docs_configuration_reference.md` | [appsettings.json などの構成ファイルとマッピング解説](.docs/docs_configuration_reference.md) |
| `docs/architecture_overview.md` | [全体アーキテクチャ構造と各層の責務定義] (./docs/architecture_overview.md)|

---
> 本プロジェクトの開発思想・AI協働方法論は[Amagi Protocol統合ドキュメント](./docs/amagiprotocol/amagi_protocol_full.md)、

