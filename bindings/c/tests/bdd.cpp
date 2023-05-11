#include "gtest/gtest.h"

extern "C" {
#include "opendal.h"
}

class OpendalBddTest : public ::testing::Test {
protected:
    // Setup code for the fixture
    opendal_operator_ptr p;
    std::string scheme;
    std::string path;
    std::string content;

    // the fixsure setup is an operator write, which will be
    // run at the beginning of every tests
    void SetUp() override
    {
        // construct the memory operator
        this->scheme = std::string("memory");
        this->path = std::string("test");
        this->content = std::string("Hello, World!");

        this->p = opendal_operator_new(scheme.c_str());

        EXPECT_TRUE(this->p);

        const opendal_bytes data = {
            .len = this->content.length(),
            .data = (uint8_t*)this->content.c_str(),
        };

        opendal_code code = opendal_operator_blocking_write(this->p, this->path.c_str(), data);

        EXPECT_EQ(code, OPENDAL_OK);
    }

    // Teardown code for the fixture, free the operator
    void TearDown() override
    {
        opendal_operator_free(&this->p);
    }
};

TEST_F(OpendalBddTest, Write)
{
    // do nothing, the fixsure does the Write Test
}

TEST_F(OpendalBddTest, Exist)
{
    opendal_result_is_exist r = opendal_operator_is_exist(this->p, this->path.c_str());

    EXPECT_EQ(r.code, OPENDAL_OK);
    EXPECT_TRUE(r.is_exist);
}

TEST_F(OpendalBddTest, EntryMode)
{
    opendal_result_stat r = opendal_operator_stat(this->p, this->path.c_str());
    assert(r.code == OPENDAL_OK);

    opendal_metadata meta = r.meta;
    EXPECT_TRUE(opendal_metadata_is_file(&meta));

    opendal_metadata_free(&meta);
}

TEST_F(OpendalBddTest, ContentLength)
{
    opendal_result_stat r = opendal_operator_stat(this->p, this->path.c_str());
    EXPECT_EQ(r.code, OPENDAL_OK);

    opendal_metadata meta = r.meta;
    EXPECT_EQ(opendal_metadata_content_length(&meta), 13);

    opendal_metadata_free(&meta);
}

TEST_F(OpendalBddTest, Read)
{
    struct opendal_result_read r = opendal_operator_blocking_read(this->p, this->path.c_str());

    EXPECT_EQ(r.code, OPENDAL_OK);
    EXPECT_EQ(r.data->len, this->content.length());

    for (int i = 0; i < r.data->len; i++) {
        EXPECT_EQ(this->content[i], (char)(r.data->data[i]));
    }

    // free the bytes's heap memory
    opendal_bytes_free(r.data);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
