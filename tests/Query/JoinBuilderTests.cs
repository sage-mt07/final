using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query;

public class JoinBuilderTests
{
    [Fact]
    public void TwoTableJoin_AllKeysMatch_ReturnsJoinedRecords()
    {
        var a = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "A1" }
        };
        var b = new List<ChildEntity>
        {
            new ChildEntity { Id = 10, ParentId = 1, Name = "B1" }
        };

        var result = a.Join(b, x => x.Id, y => y.ParentId, (x, y) => new
        {
            AName = x.Name,
            BName = y.Name
        }).ToList();

        Assert.Single(result);
        Assert.Equal("A1", result[0].AName);
        Assert.Equal("B1", result[0].BName);
    }

    [Fact]
    public void TwoTableJoin_KeyMismatch_ReturnsEmpty()
    {
        var a = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "A1" }
        };
        var b = new List<ChildEntity>
        {
            new ChildEntity { Id = 10, ParentId = 2, Name = "B1" }
        };

        var result = a.Join(b, x => x.Id, y => y.ParentId, (x, y) => new
        {
            AName = x.Name,
            BName = y.Name
        }).ToList();

        Assert.Empty(result);
    }

    [Fact]
    public void ThreeTableJoin_AllKeysMatch_ReturnsJoinedRecords()
    {
        var a = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "A1" }
        };
        var b = new List<ChildEntity>
        {
            new ChildEntity { Id = 10, ParentId = 1, Name = "B1" }
        };
        var c = new List<GrandChildEntity>
        {
            new GrandChildEntity { Id = 100, ChildId = 10, Description = "C1" }
        };

        var result = a.Join(b, x => x.Id, y => y.ParentId, (x, y) => new { x, y })
            .Join(c, ab => ab.y.Id, z => z.ChildId, (ab, z) => new
            {
                AName = ab.x.Name,
                BName = ab.y.Name,
                CDesc = z.Description
            }).ToList();

        Assert.Single(result);
        Assert.Equal("A1", result[0].AName);
        Assert.Equal("B1", result[0].BName);
        Assert.Equal("C1", result[0].CDesc);
    }

    [Fact]
    public void ThreeTableJoin_KeyMismatch_ReturnsEmpty()
    {
        var a = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "A1" }
        };
        var b = new List<ChildEntity>
        {
            new ChildEntity { Id = 10, ParentId = 1, Name = "B1" }
        };
        var c = new List<GrandChildEntity>
        {
            new GrandChildEntity { Id = 100, ChildId = 20, Description = "C1" }
        };

        var result = a.Join(b, x => x.Id, y => y.ParentId, (x, y) => new { x, y })
            .Join(c, ab => ab.y.Id, z => z.ChildId, (ab, z) => new
            {
                AName = ab.x.Name,
                BName = ab.y.Name,
                CDesc = z.Description
            }).ToList();

        Assert.Empty(result);
    }
}
