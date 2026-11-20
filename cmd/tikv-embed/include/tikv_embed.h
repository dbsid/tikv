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

#define TIKV_EMBED_PENDING 7

// Completion callback of tikv_embed_submit for requests that did not finish
// within the synchronous wait. code is TIKV_EMBED_OK (resp holds an encoded
// tikvpb.BatchCommandsResponse.Response) or TIKV_EMBED_ERROR (resp holds an
// error message). The callee releases resp with tikv_embed_free. It runs on a
// TiKV thread and must not block.
typedef void (*tikv_embed_done_fn)(uint64_t token, int32_t code, uint8_t *resp,
                                   size_t resp_len);

// Installs the completion callback. Only the first call has an effect.
void tikv_embed_set_done(tikv_embed_done_fn done);

// Starts an encoded tikvpb.BatchCommandsRequest.Request and waits at most
// wait_us for it. TIKV_EMBED_OK / TIKV_EMBED_ERROR: finished; *resp holds the
// response or an error message (release with tikv_embed_free), no callback.
// TIKV_EMBED_PENDING: the callback is called exactly once with token.
// TIKV_EMBED_UNAVAILABLE ("use gRPC", also before a callback is installed) and
// TIKV_EMBED_BAD_REQUEST: not accepted, no callback.
int32_t tikv_embed_submit(const uint8_t *req, size_t req_len, uint64_t token,
                          uint64_t wait_us, uint8_t **resp, size_t *resp_len);

// Writes the local store's health feedback (an encoded kvrpcpb.HealthFeedback,
// release with tikv_embed_free) and the interval to ask again. Returns
// TIKV_EMBED_UNAVAILABLE if the store is not ready or feedback is disabled.
int32_t tikv_embed_health_feedback(uint8_t **out, size_t *out_len,
                                   uint64_t *interval_ms);

void tikv_embed_free(uint8_t *ptr, size_t len);

// Stops TiKV and waits up to timeout_ms for it to exit.
int32_t tikv_embed_stop(bool graceful, uint64_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif // TIKV_EMBED_H
