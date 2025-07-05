using Kafka.Ksql.Linq.Application;
using Kafka.Ksql.Linq.Core.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

using System.Linq;
[Topic("sales")]
public class Sale
{
    [Key]
    public int Id { get; set; }

    [AvroTimestamp]
    public DateTime OccurredAt { get; set; }

    [DecimalPrecision(18, 2)]
    public decimal Amount { get; set; }
}

public class SalesContext : KsqlContext
{
    protected override void OnModelCreating(IModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Sale>();
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
            .BuildContext<SalesContext>();

        var message = new Sale
        {
            Id = Random.Shared.Next(),
            OccurredAt = DateTime.UtcNow,
            Amount = 20m
        };

        await context.Set<Sale>().AddAsync(message);
        // wait briefly for message to be published
        await Task.Delay(500);

        await context.Set<Sale>()
            .Window(TumblingWindow.OfMinutes(1).EmitFinal())
            .UseFinalized()
            .GroupBy(_ => 1)
            .Select(g => new { Total = g.Sum(s => s.Amount) })
            .ForEachAsync(r =>
            {
                Console.WriteLine($"Window total: {r.Total}");
                return Task.CompletedTask;
            });
    }
}
