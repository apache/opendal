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
        var x = BlockingOperator.my_add(1, 2);
        output.WriteLine("{0}", x);
    }
}
