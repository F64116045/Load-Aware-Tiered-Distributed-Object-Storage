# Tutorial 01: Object Lifecycle (PUT -> GET -> DELETE)

Scope: validate base object lifecycle and metadata/task observability.

Outcome: write one object, read it back, inspect metadata/task views, then delete it.

## 1. Start Stack

```bash
./scripts/up_stack.sh
```

Verify API health:

```bash
curl -sS http://127.0.0.1:8000/health
```

## 2. Create Test Payload

```bash
printf 'hello-object-storage\n' >/tmp/hello.bin
sha256sum /tmp/hello.bin
```

## 3. PUT Object

```bash
curl -sS -X PUT 'http://127.0.0.1:8000/v2/objects/hello-1' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @/tmp/hello.bin
```

What happens internally:

1. API selects HOT replica nodes.
2. API writes bytes to storage node `/store` endpoints.
3. Metadata is committed with new `hot_version`.
4. Due-index is written; scanner enqueues tiering task in a later pass.

## 4. GET Object

```bash
curl -sS 'http://127.0.0.1:8000/v2/objects/hello-1' -o /tmp/hello.out
cmp /tmp/hello.bin /tmp/hello.out && echo 'payload ok'
```

## 5. Inspect Admin Views

```bash
curl -sS 'http://127.0.0.1:8000/v2/admin/objects/hello-1'
curl -sS 'http://127.0.0.1:8000/v2/admin/tasks?object_id=hello-1&limit=20'
```

Checkpoints:

1. object has `current_version`.
2. task list eventually contains at least one `REPL_TO_EC` task after scanner pass.

## 6. Wait for Promotion (Optional)

```bash
watch -n 2 "curl -sS 'http://127.0.0.1:8000/v2/admin/objects/hello-1'"
```

Expected eventual state:

1. `state=EC_ACTIVE`
2. version tier becomes `EC`

## 7. DELETE Object

```bash
curl -sS -X DELETE 'http://127.0.0.1:8000/v2/objects/hello-1'
```

Verify delete:

```bash
curl -i -sS 'http://127.0.0.1:8000/v2/objects/hello-1'
```

## 8. Cleanup

```bash
rm -f /tmp/hello.bin /tmp/hello.out
```

## 9. Related Documents

1. [Request and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
2. [API Endpoints Reference](../reference/api-endpoints-reference.md)
3. [Local Setup and Smoke Validation](../operations/local-setup-and-smoke-validation.md)
