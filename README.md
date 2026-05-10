# biff.datastar

Utilities for building Datastar-driven Ring apps on Biff.

## Public API

- `com.biffweb.datastar/init-opts`
- `com.biffweb.datastar/configure-csrf`
- `com.biffweb.datastar/new-lock`
- `com.biffweb.datastar/refresh`
- `com.biffweb.datastar/wrap-datastar`
- `com.biffweb.datastar/module`

`init-opts` opens a long-lived Datastar `@get()` connection to the current page and sends:

- the `tabId` signal on the SSE request

Pair `init-opts` with `configure-csrf` to override Datastar's built-in backend actions so they include the Ring anti-forgery token:

```clj
(:require
 [com.biffweb.datastar :as biff.datastar]
 [dev.onionpancakes.chassis.core :as chassis])

[:script {:type "module"
           :src "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js"}]
[:script {:type "module"}
 (chassis/raw
  (biff.datastar/configure-csrf
   "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js"
   (:anti-forgery-token req)))]
[:div biff.datastar/init-opts
 [:form {:data-on:submit "@post('/messages')"}
  ...]]
```

`wrap-datastar` reads the `tabId` signal from Datastar request signals and attaches it to the Ring request as `:biff.datastar/tab-id`, so the default JSON request mode works without custom tab-id headers. For non-GET Datastar requests, it expects upstream middleware to have already parsed the request body into `:form-params` or `:body-params`.

## Demo

Run the demo chat app with:

```bash
clojure -M:dev:demo
```

The demo uses Jetty with virtual threads, in-memory state, Datastar-bound form fields, and the same `wrap-datastar` middleware that the library exposes. It persists per-tab UI state in the demo app itself by keying off `:biff.datastar/tab-id`.

## Tests

```bash
clojure -M:dev:test
```
