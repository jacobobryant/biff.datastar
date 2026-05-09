(ns com.biffweb.datastar-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [com.biffweb.datastar :as datastar]
    [com.biffweb.datastar.impl.brotli :as brotli]
    [ring.core.protocols :as rp])
  (:import
    (java.io PipedInputStream PipedOutputStream)))

(defn- wait-for [pred]
  (loop [remaining 100]
    (if (or (pred) (zero? remaining))
      (pred)
      (do
        (Thread/sleep 20)
        (recur (dec remaining))))))

(deftest container-opts-include-headers
  (let [opts (datastar/container-opts {:anti-forgery-token "csrf-token"})]
    (is (= datastar/tab-id-js (:data-signals:tab-id opts)))
    (is (str/includes? (:data-signals:biff-datastar-request-headers opts)
                       "'X-Biff-Datastar-Tab-ID': $tabId"))
    (is (str/includes? (:data-signals:biff-datastar-request-headers opts)
                       "'X-CSRF-Token': \"csrf-token\""))
    (is (str/includes? (:data-init opts) "@get("))
    (is (str/includes? (:data-init opts) "'X-Biff-Datastar-Tab-ID': $tabId"))
    (is (str/includes? (:data-init opts) "'X-Biff-Datastar-Page-Request': 'true'"))
    (is (str/includes? (:data-init opts) "'X-CSRF-Token': \"csrf-token\""))))

(deftest default-action-headers-script-wraps-fetch-actions
  (let [script (datastar/default-action-headers-script "https://example.test/datastar.js")]
    (is (str/includes? script "import { action, actions, root } from \"https://example.test/datastar.js\";"))
    (is (str/includes? script "root.biffDatastarRequestHeaders"))
    (is (str/includes? script "const methods = ['get', 'post', 'put', 'patch', 'delete'];"))
    (is (str/includes? script "headers: {"))
    (is (str/includes? script "...defaultHeaders()"))
    (is (str/includes? script "...(options.headers ?? {})"))))

(deftest refresh-bumps-epoch
  (let [lock-state (datastar/new-lock)]
    (is (= 0 @(:biff.datastar/epoch lock-state)))
    (datastar/refresh lock-state)
    (is (= 1 @(:biff.datastar/epoch lock-state)))))

(deftest wrap-datastar-persists-tab-state-on-normal-response
  (let [store (atom {})
        handler (datastar/wrap-datastar
                 (fn [req]
                   {:status 204
                    :biff.datastar/tab-state (assoc (or (:biff.datastar/tab-state req) {}) :channel-id "general")}))
        request (merge (datastar/new-lock)
                       {:request-method :post
                        :headers {"x-biff-datastar-tab-id" "tab-1"}
                        :biff.datastar/get-user-id (constantly "user-1")
                        :biff.datastar/get-tab-state (fn [_ _ tab-id] (get @store tab-id))
                        :biff.datastar/set-tab-state (fn [_ _ tab-id tab-state]
                                                       (swap! store assoc tab-id tab-state))})]
    (handler request)
    (is (= {:channel-id "general"} (@store "tab-1")))))

(deftest sse-response-streams-initial-patch-and-touches-last-seen
  (let [store (atom {"tab-1" {:channel-id "general"}})
        rendered (atom "<div id=\"biff-datastar-content\">Hello</div>")
        handler (datastar/wrap-datastar
                 (fn [req]
                   {:status 200
                    :body @rendered
                    :biff.datastar/tab-state (assoc (or (:biff.datastar/tab-state req) {}) :channel-id "general")}))
        request (merge (datastar/new-lock)
                       {:request-method :get
                        :headers {"x-biff-datastar-tab-id" "tab-1"
                                  "x-biff-datastar-page-request" "true"
                                  "accept" "text/event-stream"
                                  "datastar-request" "true"}
                        :biff.datastar/get-user-id (constantly "user-1")
                        :biff.datastar/get-tab-state (fn [_ _ tab-id] (get @store tab-id))
                        :biff.datastar/set-tab-state (fn [_ _ tab-id tab-state]
                                                       (swap! store assoc tab-id tab-state))})
         response (handler request)
         body (:body response)]
    (is (= 200 (:status response)))
    (is (= "br" (get-in response [:headers "Content-Encoding"])))
    (let [out (PipedOutputStream.)
          in (PipedInputStream. out 65536)
          writer (future (rp/write-body-to-stream body nil out))]
      (try
        (is (wait-for #(contains? (get @store "tab-1") :biff.datastar/last-seen)))
        (is (wait-for #(pos? (.available in))))
        (let [buf (byte-array (.available in))]
          (.read in buf)
          (is (str/includes? (brotli/decompress-stream buf) "event: datastar-patch-elements")))
        (finally
          (datastar/refresh request)
          (.close in)
          (.close out)
          (future-cancel writer))))))
