(ns com.biffweb.datastar
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.biffweb.datastar.impl.brotli :as brotli]
    [ring.core.protocols :as rp])
  (:import
    (java.time Duration Instant)
    (java.util.concurrent TimeUnit)
    (java.util.concurrent.locks Condition ReentrantLock)))

(def ^:private heartbeat-interval (Duration/ofMinutes 5))
(def ^:private default-context-window 18)
(def ^:private default-rate-limit-ms 15)
(def ^:private default-datastar-script-url
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js")
(def tab-id-js
  "self.crypto.randomUUID().substring(0,8)")

(defn- js-string [s]
  (pr-str s))

(defn- headers-expr [fields]
  (str "({" (str/join ", " fields) "})"))

(def ^:private connection-action
  (let [headers ["'X-Biff-Datastar-Page-Request': 'true'"]
        options (str "{headers: " (headers-expr headers) ", "
                       "openWhenHidden: true, "
                       "retryMaxCount: Infinity}")]
    ;; The throwaway `u` param keeps this expression valid whether or not the
    ;; page already has its own query string; the backend ignores it.
    (str "@get("
         "window.location.pathname + (window.location.search + '&u=').replace(/^&/,'?'), "
         options
         ")")))

(defn container-opts
  "Returns Datastar attributes for the container element that owns the page-level
  SSE connection."
  [req]
  (let [action connection-action]
    {:data-signals:tab-id tab-id-js
     :data-init action
     :data-on:online__window action}))

(defn configure-csrf
  "Returns a JS module string that adds the Ring anti-forgery token to Datastar's
  backend actions."
  ([csrf-token]
   (configure-csrf default-datastar-script-url csrf-token))
  ([datastar-script-url csrf-token]
   (str "import { action, actions } from "
        (js-string datastar-script-url)
        ";\n"
         "const methods = ['get', 'post', 'put', 'patch', 'delete'];\n"
          "const originals = Object.fromEntries(methods.map(name => [name, actions[name]]));\n"
          "const csrfHeader = "
          (headers-expr [(str "'X-CSRF-Token': " (js-string csrf-token))])
         ";\n"
         "const mergeOptions = (options = {}) => ({\n"
         "  ...options,\n"
         "  headers: {\n"
         "    ...csrfHeader,\n"
         "    ...(options.headers ?? {}),\n"
         "  },\n"
         "});\n"
        "for (const name of methods) {\n"
        "  const original = originals[name];\n"
        "  if (!original) continue;\n"
        "  action({\n"
        "    name,\n"
        "    apply(ctx, url, options = {}) {\n"
        "      return original(ctx, url, mergeOptions(options));\n"
        "    },\n"
         "  });\n"
         "}\n")))

(defn- signal-key [k]
  (if (or (keyword? k) (string? k) (symbol? k))
    (keyword (name k))
    k))

(defn- normalize-signals [signals]
  (into {}
        (map (fn [[k v]]
               [(signal-key k) v]))
        signals))

(defn- parse-signals [signals]
  (cond
    (nil? signals) nil
    (map? signals) (normalize-signals signals)
    (string? signals) (when-let [signals (some-> signals str/trim not-empty)]
                        (json/read-str signals :key-fn keyword))
    :else
    (throw (ex-info "Datastar request signals must already be parsed into a map or be provided as a JSON string."
                    {:signals-type (type signals)}))))

(defn- request-signals [req]
  (or (get-in req [:params "datastar"])
      (when (= "true" (get-in req [:headers "datastar-request"]))
        (or (not-empty (:form-params req))
            (not-empty (:body-params req))
            (:params req)))))

(defn- attach-signals [req]
  (if-some [signals (some-> (request-signals req) parse-signals)]
    (assoc req :biff.datastar/signals signals)
    req))

(defn new-lock
  "Creates the lock state expected by `refresh` and `wrap-datastar`."
  []
  (let [lock (ReentrantLock.)]
    {:biff.datastar/lock lock
     :biff.datastar/condition (.newCondition lock)
     :biff.datastar/epoch (atom 0)}))

(defn refresh
  "Signals all live Datastar SSE loops to rerender."
  [{:biff.datastar/keys [lock condition epoch]}]
  (when-not (and lock condition epoch)
    (throw (ex-info "refresh requires :biff.datastar/lock, :biff.datastar/condition, and :biff.datastar/epoch."
                    {})))
  (.lock ^ReentrantLock lock)
  (try
    (swap! epoch inc)
    (.signalAll ^Condition condition)
    nil
    (finally
      (.unlock ^ReentrantLock lock))))

(defn- read-tab-state [req user-id tab-id]
  (when tab-id
    (cond
      (:biff.datastar/get-tab-state req)
      ((:biff.datastar/get-tab-state req) req user-id tab-id)

      (:biff.db/get-kv req)
      ((:biff.db/get-kv req) req :biff.datastar/tab-state (str user-id ":" tab-id))

      :else
      (throw (ex-info "wrap-datastar requires :biff.datastar/get-tab-state or :biff.db/get-kv when a tab id is present."
                      {:tab-id tab-id})))))

(defn- write-tab-state! [req user-id tab-id tab-state]
  (when tab-id
    (cond
      (:biff.datastar/set-tab-state req)
      ((:biff.datastar/set-tab-state req) req user-id tab-id tab-state)

      (:biff.db/set-kv req)
      ((:biff.db/set-kv req) req :biff.datastar/tab-state (str user-id ":" tab-id) tab-state)

      :else
      (throw (ex-info "wrap-datastar requires :biff.datastar/set-tab-state or :biff.db/set-kv when a tab id is present."
                      {:tab-id tab-id})))))

(defn- user-id [req]
  (if-let [get-user-id (:biff.datastar/get-user-id req)]
    (get-user-id req)
    (get-in req [:session :uid])))

(defn- attach-tab-state [req]
  (let [tab-id (get-in req [:biff.datastar/signals :tabId])
        user-id* (user-id req)
        tab-state (read-tab-state req user-id* tab-id)]
    (assoc req
           :biff.datastar/tab-id tab-id
           :biff.datastar/user-id user-id*
           :biff.datastar/tab-state tab-state)))

(defn- response-map [response]
  (when-not (map? response)
    (throw (ex-info "Datastar handlers must return Ring response maps."
                    {:response response})))
  response)

(defn- response-tab-state [req response]
  (if (contains? response :biff.datastar/tab-state)
    (:biff.datastar/tab-state response)
    (:biff.datastar/tab-state req)))

(defn- persist-tab-state! [req response]
  (let [tab-id (:biff.datastar/tab-id req)
        before (:biff.datastar/tab-state req)
        after (response-tab-state req response)]
    (when (and tab-id (not= before after))
      (write-tab-state! req (:biff.datastar/user-id req) tab-id after))))

(defn- datastar-sse-request? [req]
  (and (= :get (:request-method req))
       (= "true" (get-in req [:headers "x-biff-datastar-page-request"]))))

(defn- patch-elements-event [body]
  (str "event: datastar-patch-elements\n"
       "id: " (Integer/toHexString (hash body)) "\n"
       "data: elements " (str/replace body "\n" "\ndata: elements ")
       "\n\n"))

(defn- wait-for-update! [{:biff.datastar/keys [lock condition epoch]} last-epoch timeout-ms]
  (.lock ^ReentrantLock lock)
  (try
    (loop [remaining (.toNanos TimeUnit/MILLISECONDS (long (max 1 timeout-ms)))]
      (let [current @epoch]
        (cond
          (not= current last-epoch) current
          (<= remaining 0) current
          :else (recur (.awaitNanos ^Condition condition remaining)))))
    (finally
      (.unlock ^ReentrantLock lock))))

(defn- millis-until-heartbeat [last-seen-at]
  (if last-seen-at
    (let [elapsed (.toMillis (Duration/between last-seen-at (Instant/now)))
          interval (.toMillis heartbeat-interval)]
      (max 1 (- interval elapsed)))
    (.toMillis heartbeat-interval)))

(defn- touch-last-seen! [req]
  (when (and (:biff.datastar/tab-id req)
             (:biff.datastar/tab-state req))
    (let [tab-state (:biff.datastar/tab-state req)
          updated (assoc tab-state :biff.datastar/last-seen (Instant/now))]
      (write-tab-state! req (:biff.datastar/user-id req) (:biff.datastar/tab-id req) updated)
      (:biff.datastar/last-seen updated))))

(defn- current-request [req]
  (-> req
      attach-signals
      attach-tab-state
      (assoc :biff.datastar/sse-request true)))

(defrecord ^:private StreamResponse [write-fn]
  rp/StreamableResponseBody
  (write-body-to-stream [_ _ output-stream]
    (write-fn output-stream)))

(defn- stream-response-body [handler req]
  (let [context-window (or (:biff.datastar/context-window req) default-context-window)
        rate-limit-ms (long (or (:biff.datastar/rate-limit-ms req) default-rate-limit-ms))]
    (when-not (pos? rate-limit-ms)
      (throw (ex-info ":biff.datastar/rate-limit-ms must be positive."
                      {:rate-limit-ms rate-limit-ms})))
    (->StreamResponse
      (fn [raw-out]
        (try
          (with-open [scratch (brotli/byte-array-out-stream)
                      br (brotli/compress-out-stream scratch :window-size context-window)]
            (loop [last-body-hash nil
                   last-seen-at nil
                   seen-epoch @(:biff.datastar/epoch req)]
              (let [render-start-ms (System/currentTimeMillis)
                    current-req (current-request req)
                    response (response-map (handler current-req))
                    body (or (:body response) "")
                    body-hash (hash body)
                    send? (and (seq body) (not= body-hash last-body-hash))
                    _ (when send?
                        (.write raw-out (brotli/compress-stream scratch br (patch-elements-event body)))
                        (.flush raw-out))
                    _ (persist-tab-state! current-req response)
                    last-seen-at (if (or (nil? last-seen-at)
                                         (>= (.toMillis (Duration/between last-seen-at (Instant/now)))
                                             (.toMillis heartbeat-interval)))
                                   (or (touch-last-seen! current-req) last-seen-at)
                                   last-seen-at)
                    keepalive-ms (millis-until-heartbeat last-seen-at)
                    seen-epoch (wait-for-update! req seen-epoch keepalive-ms)
                    elapsed-ms (- (System/currentTimeMillis) render-start-ms)]
                (when (< elapsed-ms rate-limit-ms)
                  (Thread/sleep (- rate-limit-ms elapsed-ms)))
                (recur (if send? body-hash last-body-hash)
                       last-seen-at
                       seen-epoch))))
          (catch Throwable t
            (if (or (instance? InterruptedException t)
                    (= "Pipe closed" (.getMessage t)))
              (log/debug t "Datastar SSE stream closed after the client disconnected.")
              (log/error t "Datastar SSE loop crashed"))))))))

(defn wrap-datastar
  "Ring middleware that attaches tab state and upgrades Datastar SSE GET requests
  into long-lived compressed event streams."
  ([handler]
   (wrap-datastar handler nil))
  ([handler params]
   (fn [req]
    (let [req (-> (merge req params)
                  attach-signals
                  attach-tab-state)]
       (if (datastar-sse-request? req)
         (do
           (when-not (and (:biff.datastar/lock req)
                         (:biff.datastar/condition req)
                         (:biff.datastar/epoch req))
            (throw (ex-info "Datastar SSE requests require the keys returned by new-lock in the request."
                            {})))
           {:status 200
            :headers {"Content-Type" "text/event-stream; charset=utf-8"
                      "Cache-Control" "no-store"
                       "Content-Encoding" "br"}
             :body (stream-response-body handler req)})
          (let [response (response-map (handler req))]
            (persist-tab-state! req response)
             response))))))

(defn module
  []
  {:biff.ring/site-middleware [wrap-datastar]
   :biff.db/on-tx #'refresh
   :biff.core/init (fn [_] (new-lock))})

(defn purge-tab-state!
  [{:biff.db/keys [get-kv set-kv list-kv] :as ctx} last-seen-before]
  (when-not (and get-kv set-kv list-kv)
    (throw (ex-info "purge-tab-state! requires :biff.db/get-kv, :biff.db/set-kv, and :biff.db/list-kv."
                    {})))
  (doseq [key (list-kv ctx :biff.datastar/tab-state nil)]
    (let [tab-state (get-kv ctx :biff.datastar/tab-state key)
          last-seen (:biff.datastar/last-seen tab-state)]
      (when (or (nil? last-seen)
                (.isBefore ^Instant last-seen ^Instant last-seen-before))
        (set-kv ctx :biff.datastar/tab-state key nil))))
  nil)
