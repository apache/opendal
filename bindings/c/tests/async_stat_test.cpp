#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // For usleep

// Include the generated OpenDAL C header
#include "opendal.h"

int main(int argc, char *argv[]) {
    printf("Starting OpenDAL C async stat test...\n");

    // 1. Create Operator
    // Use memory backend for testing, no options needed
    opendal_result_operator_new result_op = opendal_async_operator_new("memory", NULL);
    assert(result_op.error == NULL);

    // IMPORTANT: Cast the operator pointer to the async type
    opendal_async_operator *op = (opendal_async_operator *)result_op.op;
    assert(op != NULL);
    printf("Async Operator created successfully.\n");

    // 2. Call async stat for a non-existent file
    const char *path = "non_existent_file.txt";
    printf("Calling async stat for path: %s\n", path);
    opendal_future_stat *future_stat = opendal_async_operator_stat(op, path);
    assert(future_stat != NULL);
    printf("Async stat future created.\n");

    // 3. Poll the future until it's ready
    printf("Polling future status...\n");
    opendal_future_poll_status status;
    int poll_count = 0;
    while ((status = opendal_future_stat_poll(future_stat)) == PENDING) {
        poll_count++;
        // Add a small delay to avoid busy-waiting (e.g., 10 milliseconds)
        // In a real application, you might integrate this with an event loop.
        usleep(10000);
    }
    printf("Future is READY after %d polls.\n", poll_count);
    assert(status == READY);

    // 4. Get the result
    printf("Getting future result...\n");
    opendal_result_stat result_stat = opendal_future_stat_get(future_stat);

    // 5. Verify the result (should be NotFound error)
    assert(result_stat.meta == NULL); // Meta should be NULL for error
    assert(result_stat.error != NULL); // Error should be non-NULL

    opendal_code error_code = result_stat.error->code;
    const char *error_message = (const char*)result_stat.error->message.data;

    printf("Received expected error: Code %d, Message: %s\n", error_code, error_message); // Be careful with %s if not null-terminated
    assert(error_code == OPENDAL_NOT_FOUND);

    // 6. Free resources
    printf("Freeing resources...\n");
    opendal_error_free(result_stat.error);
    opendal_future_stat_free(future_stat);
    opendal_async_operator_free(op); // Use the async free function

    printf("OpenDAL C async stat test finished successfully!\n");

    return 0;
}
