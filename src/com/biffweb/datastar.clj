(ns com.biffweb.datastar
  (:require
   [clojure.string :as str]
   [com.biffweb.datastar.brotli :as brotli]
   [dev.onionpancakes.chassis.core :as chassis])
  (:import
   (java.io BufferedOutputStream PipedInputStream PipedOutputStream)
   (java.time Duration Instant)
   (java.util.concurrent TimeUnit)
   (java.util.concurrent.locks Condition ReentrantLock)))

(def ^:private tab-state-namespace :biff.datastar/tab-state)
(def ^:private tab-id-header "x-biff-datastar-tab-id")
(def ^:private keepalive-interval (Duration/ofMinutes 5))
(def ^:private default-context-window 18)
(def ^:private default-rate-limit-fps 60)
(def ^:private pipe-buffer-size 65536)

(def tab-id-js
  "self.crypto.randomUUID().substring(0,8)")

(defn- js-string [s]
  (pr-str s))

(defn- connection-action [req]
  (let [headers (cond-> ["'X-Biff-Datastar-Tab-ID': $tabId"]
                  (:anti-forgery-token req)
                  (conj (str "'X-CSRF-Token': " (js-string (:anti-forgery-token req)))))
        options (str "{headers: {" (str/join ", " headers) "}, "
                     "openWhenHidden: true, "
                     "retryMaxCount: Infinity}")]
    (str "@get("
         "window.location.pathname + (window.location.search + '&u=').replace(/^&/,'?'), "
         options
         ")")))

(defn container-opts
  "Returns Datastar attributes for the container element that owns the page-level
  SSE connection."
  [req]
  (let [action (connection-action req)]
    {:data-signals:tab-id tab-id-js
     :data-init action
     :data-on:online__window action}))

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
      ((:biff.db/get-kv req) req tab-state-namespace (str user-id ":" tab-id))

      :else
      (throw (ex-info "wrap-datastar requires :biff.datastar/get-tab-state or :biff.db/get-kv when a tab id is present."
                      {:tab-id tab-id})))))

(defn- write-tab-state! [req user-id tab-id tab-state]
  (when tab-id
    (cond
      (:biff.datastar/set-tab-state req)
      ((:biff.datastar/set-tab-state req) req user-id tab-id tab-state)

      (:biff.db/set-kv req)
      ((:biff.db/set-kv req) req tab-state-namespace (str user-id ":" tab-id) tab-state)

      :else
      (throw (ex-info "wrap-datastar requires :biff.datastar/set-tab-state or :biff.db/set-kv when a tab id is present."
                      {:tab-id tab-id})))))

(defn- user-id [req]
  (if-let [get-user-id (:biff.datastar/get-user-id req)]
    (get-user-id req)
    (get-in req [:session :uid])))

(defn- attach-tab-state [req]
  (let [tab-id (get-in req [:headers tab-id-header])
        user-id* (user-id req)
        tab-state (read-tab-state req user-id* tab-id)]
    (assoc req
           :biff.datastar/tab-id tab-id
           :biff.datastar/user-id user-id*
           :biff.datastar/tab-state tab-state)))

(defn- normalize-response [response]
  (cond
    (map? response) response
    (nil? response) {:status 204}
    :else {:status 200 :body response}))

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
       (= "true" (get-in req [:headers "datastar-request"]))
       (str/includes? (str/lower-case (get-in req [:headers "accept"] "")) "text/event-stream")))

(defn- render-body [body]
  (cond
    (nil? body) ""
    (string? body) body
    :else (chassis/html body)))

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

(defn- millis-until-keepalive [last-seen-at]
  (if last-seen-at
    (let [elapsed (.toMillis (Duration/between last-seen-at (Instant/now)))
          interval (.toMillis keepalive-interval)]
      (max 1 (- interval elapsed)))
    1))

(defn- touch-last-seen! [req]
  (when (:biff.datastar/tab-id req)
    (let [tab-state (or (:biff.datastar/tab-state req) {})
          updated (assoc tab-state :biff.datastar/last-seen (Instant/now))]
      (write-tab-state! req (:biff.datastar/user-id req) (:biff.datastar/tab-id req) updated)
      (:biff.datastar/last-seen updated))))

(defn- current-request [req]
  (-> req
      attach-tab-state
      (assoc :biff.datastar/sse-request true)))

(defn- pipe-response-body [handler req]
  (let [pipe-in (PipedInputStream. pipe-buffer-size)
        pipe-out (PipedOutputStream. pipe-in)
        context-window (or (:biff.datastar/context-window req) default-context-window)
        rate-limit-fps (or (:biff.datastar/rate-limit-fps req) default-rate-limit-fps)
        min-frame-ms (long (max 1 (Math/round (/ 1000.0 rate-limit-fps))))]
    (when-not (pos? rate-limit-fps)
      (throw (ex-info ":biff.datastar/rate-limit-fps must be positive."
                      {:rate-limit-fps rate-limit-fps})))
    (future
      (with-open [raw-out pipe-out
                  out (BufferedOutputStream. raw-out)
                  scratch (brotli/byte-array-out-stream)
                  br (brotli/compress-out-stream scratch :window-size context-window)]
        (loop [last-body nil
               last-send-ms 0
               last-seen-at nil
               seen-epoch @(:biff.datastar/epoch req)]
          (let [last-seen-at (or last-seen-at (touch-last-seen! (current-request req)))
                current-req (current-request req)
                response (normalize-response (handler current-req))
                _ (persist-tab-state! current-req response)
                body (render-body (:body response))
                now-ms (System/currentTimeMillis)
                next-send-ms (if (and (not= body last-body) (seq body))
                               (do
                                 (when (< (- now-ms last-send-ms) min-frame-ms)
                                   (Thread/sleep (- min-frame-ms (- now-ms last-send-ms))))
                                 (.write out (brotli/compress-stream scratch br (patch-elements-event body)))
                                 (.flush out)
                                 (System/currentTimeMillis))
                               last-send-ms)
                last-body (if (and (not= body last-body) (seq body)) body last-body)
                seen-epoch @(:biff.datastar/epoch req)
                keepalive-ms (millis-until-keepalive last-seen-at)
                seen-epoch (wait-for-update! req seen-epoch keepalive-ms)
                last-seen-at (if (>= (.toMillis (Duration/between last-seen-at (Instant/now)))
                                    (.toMillis keepalive-interval))
                               (touch-last-seen! (current-request req))
                               last-seen-at)]
            (recur last-body next-send-ms last-seen-at seen-epoch)))))
    pipe-in))

(defn wrap-datastar
  "Ring middleware that attaches tab state and upgrades Datastar SSE GET requests
  into long-lived compressed event streams."
  [handler]
  (fn [req]
    (let [req (attach-tab-state req)]
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
           :body (pipe-response-body handler req)})
        (let [response (normalize-response (handler req))]
          (persist-tab-state! req response)
          response)))))

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
  (doseq [key (list-kv ctx tab-state-namespace nil)]
    (let [tab-state (get-kv ctx tab-state-namespace key)
          last-seen (:biff.datastar/last-seen tab-state)]
      (when (or (nil? last-seen)
                (.isBefore ^Instant last-seen ^Instant last-seen-before))
        (set-kv ctx tab-state-namespace key nil))))
  nil)
