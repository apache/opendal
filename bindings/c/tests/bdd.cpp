#include "gtest/gtest.h"

extern "C" {
#include "opendal.h"
}

class OperatorFixture : public ::testing::Test {
protected:
    // Setup code for the fixture
    void SetUp() override
    {
        // todo
    }

    // Teardown code for the fixture
    void TearDown() override
    {
        // todo
    }
};

TEST(OpendalBddTest, Write)
{
    // todo
}

TEST(OpendalBddTest, Exist)
{
    // todo
}

TEST(OpendalBddTest, EntryMode)
{
    // todo
}

TEST(OpendalBddTest, ContentLength)
{
    // todo
}

TEST(OpendalBddTest, Read)
{
    // todo
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
