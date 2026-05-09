# biff.datastar

Utilities for building Datastar-driven Ring apps on Biff.

## Public API

- `com.biffweb.datastar/container-opts`
- `com.biffweb.datastar/new-lock`
- `com.biffweb.datastar/refresh`
- `com.biffweb.datastar/wrap-datastar`
- `com.biffweb.datastar/module`
- `com.biffweb.datastar/purge-tab-state!`

`container-opts` opens a long-lived Datastar `@get()` connection to the current page and sends:

- `X-Biff-Datastar-Tab-ID` on the SSE request
- `X-CSRF-Token` when `:anti-forgery-token` is present on the Ring request

Datastar does not currently expose a global "send these headers on every request" setting, so non-SSE Datastar actions still need to set their own headers explicitly.

## Demo

Run the demo chat app with:

```bash
clojure -M:demo
```

The demo uses Jetty with virtual threads, in-memory state, Datastar-bound form fields, and the same `wrap-datastar` middleware that the library exposes.

## Tests

```bash
clojure -M:test
```
