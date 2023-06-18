using Xunit.Abstractions;

namespace DotOpenDAL.Tests;

public class UnitTest1
{

    private readonly ITestOutputHelper output;

    public UnitTest1(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    public void Test1()
    {
        output.WriteLine("{0}", 42);
    }
}
