using System.Linq;
using Kafka.Ksql.Linq.Query.Builders.Functions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Builders.Functions;

public class KsqlFunctionRegistryTests
{
    [Theory]
    [InlineData("Sum", "DOUBLE")]
    [InlineData("Count", "BIGINT")]
    [InlineData("Max", "ANY")]
    [InlineData("Min", "ANY")]
    [InlineData("TopK", "ARRAY")]
    [InlineData("Histogram", "MAP")]
    [InlineData("FooBar", "UNKNOWN")]
    [InlineData("sum", "DOUBLE")]
    [InlineData("SuM", "DOUBLE")]
    public void InferTypeFromMethodName_ReturnsExpected(string methodName, string expected)
    {
        var result = KsqlFunctionRegistry.InferTypeFromMethodName(methodName);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void GetSpecialHandlingFunctions_IncludesParseAndConvert()
    {
        var result = KsqlFunctionRegistry.GetSpecialHandlingFunctions();
        Assert.Contains("Parse", result);
        Assert.Contains("Convert", result);
    }

    [Fact]
    public void GetSpecialHandlingFunctions_OnlyReturnsRequiresSpecial()
    {
        var expected = KsqlFunctionRegistry.GetAllMappings()
            .Where(m => m.Value.RequiresSpecialHandling)
            .Select(m => m.Key)
            .ToHashSet();

        var result = KsqlFunctionRegistry.GetSpecialHandlingFunctions();
        Assert.True(result.SetEquals(expected));
    }

    [Fact]
    public void GetFunctionsByCategory_ContainsAllCategories()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var expected = new[]
        {
            "String", "Math", "Date", "Aggregate", "Array", "JSON",
            "Cast", "Conditional", "URL", "GEO", "Crypto", "Window"
        };

        foreach (var key in expected)
        {
            Assert.Contains(key, categories.Keys);
        }
    }

    [Fact]
    public void GetFunctionsByCategory_StringCategoryContents()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var list = categories["String"];

        Assert.Contains("ToUpper", list);
        Assert.Contains("Trim", list);
        Assert.Contains("Contains", list);
    }

    [Fact]
    public void GetFunctionsByCategory_AggregateCategoryContents()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();
        var list = categories["Aggregate"];

        Assert.Contains("Sum", list);
        Assert.Contains("Count", list);
        Assert.Contains("Max", list);
    }

    [Fact]
    public void GetFunctionsByCategory_NoEmptyLists()
    {
        var categories = KsqlFunctionRegistry.GetFunctionsByCategory();

        foreach (var kvp in categories)
        {
            Assert.True(kvp.Value.Count > 0);
        }
    }
}
