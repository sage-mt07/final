# 🧪 Query系 DSL 複合パターン テスト指示（追加）

## 11. 3テーブル結合（Join + Join）パターン

```csharp
context.Orders
    .Join(context.Customers)
    .On((o, c) => o.CustomerId == c.Id)
    .Join(context.Payments)
    .On((oc, p) => oc.o.Id == p.OrderId)
    .Select((ocp) => new { ocp.oc.o.Id, ocp.oc.c.Name, ocp.p.Status });
```

🧭 対象処理範囲：JoinBuilder による多段結合（Joinのネスト構造）解析対応の確認
