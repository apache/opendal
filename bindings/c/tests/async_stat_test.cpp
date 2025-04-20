#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // For usleep
#include "gtest/gtest.h"
// Include the generated OpenDAL C header
extern "C" {
    #include "opendal.h"
    }
    
class OpendalAsyncStatTest : public ::testing::Test {
    protected:
        const opendal_async_operator* op;
    
        void SetUp() override
        {
            opendal_result_operator_new result_op = opendal_async_operator_new("memory", NULL);
            EXPECT_TRUE(result_op.error == nullptr);
            EXPECT_TRUE(result_op.op != nullptr);

            this->op = (opendal_async_operator *)result_op.op;
            EXPECT_TRUE(this->op);
        }
    
        void TearDown() override {
            opendal_async_operator_free(this->op); // Use the async free function
        }
    };

TEST_F(OpendalAsyncStatTest, AsyncStatTest) {
    // Call async stat for a non-existent file
    const char *path = "non_existent_file.txt";
    opendal_future_stat *future_stat = opendal_async_operator_stat(this->op, path);
    EXPECT_TRUE(future_stat != nullptr);

    // Poll the future until it's ready
    opendal_future_poll_status status;
    int poll_count = 0;
    while ((status = opendal_future_stat_poll(future_stat)) == PENDING) {
        poll_count++;
        // Add a small delay to avoid busy-waiting (e.g., 10 milliseconds)
        // In a real application, you might integrate this with an event loop.
        usleep(10000);
    }
    assert(status == READY);

    // Get the result
    opendal_result_stat result_stat = opendal_future_stat_get(future_stat);

    // Verify the result (should be NotFound error)
    EXPECT_TRUE(result_stat.meta == nullptr); // Meta should be NULL for error
    EXPECT_TRUE(result_stat.error != nullptr); // Error should be non-NULL

    opendal_code error_code = result_stat.error->code;
    const char *error_message = (const char*)result_stat.error->message.data;

    EXPECT_TRUE(error_code == OPENDAL_NOT_FOUND);
    // Free resources
    opendal_error_free(result_stat.error);
    opendal_future_stat_free(future_stat);
}
