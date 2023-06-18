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
        var op = new BlockingOperator();
        output.WriteLine("{0}", op.Op.ToString());
        op.Write("test");
        var result = op.Read("test");
        output.WriteLine("{0}", result);
    }
}
