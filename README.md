# biff.datastar

Utilities for building Datastar-driven Ring apps on Biff.

## Public API

- `com.biffweb.datastar/container-opts`
- `com.biffweb.datastar/configure-csrf`
- `com.biffweb.datastar/new-lock`
- `com.biffweb.datastar/refresh`
- `com.biffweb.datastar/wrap-datastar`
- `com.biffweb.datastar/module`
- `com.biffweb.datastar/purge-tab-state!`

`container-opts` opens a long-lived Datastar `@get()` connection to the current page and sends:

- the `tabId` signal on the SSE request

Pair `container-opts` with `configure-csrf` to override Datastar's built-in backend actions so they include the Ring anti-forgery token:

```clj
[:script {:type "module"
           :src "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js"}]
[:script {:type "module"}
 (datastar/configure-csrf
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js"
  (:anti-forgery-token req))]
[:div (datastar/container-opts req)
 [:form {:data-on:submit "@post('/messages')"}
  ...]]
```

`wrap-datastar` reads the `tabId` signal from Datastar request signals, so the default JSON request mode works without custom tab-id headers. For non-GET Datastar requests, it expects upstream middleware to have already parsed the request body into `:form-params` or `:body-params`.

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
