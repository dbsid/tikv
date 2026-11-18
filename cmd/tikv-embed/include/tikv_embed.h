// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
//
// C ABI of the embedded TiKV library (cmd/tikv-embed).
#ifndef TIKV_EMBED_H
#define TIKV_EMBED_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TIKV_EMBED_OK 0
#define TIKV_EMBED_UNAVAILABLE 1
#define TIKV_EMBED_BAD_REQUEST 2
#define TIKV_EMBED_ERROR 3
#define TIKV_EMBED_TIMEOUT 4
#define TIKV_EMBED_EXITED 5
#define TIKV_EMBED_ALREADY_STARTED 6

// Starts TiKV on a background thread. argv uses tikv-server flags without the
// program name, e.g. {"--config=tikv.toml", "--addr=127.0.0.1:20160", ...}.
int32_t tikv_embed_start(const char *const *argv, int32_t argc);

// Waits until the local store serves requests.
int32_t tikv_embed_wait_ready(uint64_t timeout_ms);

// Returns the local store ID, or 0 before it is known.
uint64_t tikv_embed_store_id(void);

// Executes an encoded tikvpb.BatchCommandsRequest.Request. On TIKV_EMBED_OK
// *resp holds an encoded tikvpb.BatchCommandsResponse.Response; on
// TIKV_EMBED_ERROR / TIKV_EMBED_BAD_REQUEST it holds an error message. Release
// *resp with tikv_embed_free. TIKV_EMBED_UNAVAILABLE means "use gRPC".
int32_t tikv_embed_call(const uint8_t *req, size_t req_len, uint8_t **resp,
                        size_t *resp_len);

void tikv_embed_free(uint8_t *ptr, size_t len);

// Stops TiKV and waits up to timeout_ms for it to exit.
int32_t tikv_embed_stop(bool graceful, uint64_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif // TIKV_EMBED_H
