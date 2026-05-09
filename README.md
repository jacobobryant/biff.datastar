# biff.datastar

Utilities for building Datastar-driven Ring apps on Biff.

## Public API

- `com.biffweb.datastar/container-opts`
- `com.biffweb.datastar/default-action-headers-script`
- `com.biffweb.datastar/new-lock`
- `com.biffweb.datastar/refresh`
- `com.biffweb.datastar/wrap-datastar`
- `com.biffweb.datastar/module`
- `com.biffweb.datastar/purge-tab-state!`

`container-opts` opens a long-lived Datastar `@get()` connection to the current page and sends:

- `X-Biff-Datastar-Tab-ID` on the SSE request
- `X-CSRF-Token` when `:anti-forgery-token` is present on the Ring request

`container-opts` also publishes a `biffDatastarRequestHeaders` page signal with those default headers. Pair it with `default-action-headers-script` to override Datastar's built-in `@get`, `@post`, `@put`, `@patch`, and `@delete` actions so they merge those page-level headers automatically:

```clj
[:script {:type "module"
          :src "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js"}]
[:script {:type "module"}
 (datastar/default-action-headers-script
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js")]
[:div (datastar/container-opts req)
 [:form {:data-on:submit "@post('/messages', {contentType: 'form'})"}
  ...]]
```

## Demo

Run the demo chat app with:

```bash
clojure -M:dev:demo
```

The demo uses Jetty with virtual threads, in-memory state, Datastar-bound form fields, and the same `wrap-datastar` middleware that the library exposes.

## Tests

```bash
clojure -M:dev:test
```
