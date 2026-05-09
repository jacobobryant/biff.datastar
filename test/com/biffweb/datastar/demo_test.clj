(ns com.biffweb.datastar.demo-test
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [com.biffweb.datastar.demo :as demo]))

(deftest demo-page-uses-published-datastar-bundle
  (let [response (demo/app {:request-method :get
                            :uri "/"
                            :headers {}
                            :query-params {}})
        body (:body response)]
    (is (= 200 (:status response)))
    (is (str/includes? body demo/datastar-script-url))
    (is (str/includes? body "data-signals:message-count"))
    (is (str/includes? body "requestAnimationFrame(() =&gt; { el.scrollTop = el.scrollHeight; })"))))

(deftest send-message-action-clears-message-signal
  (let [previous-state @demo/app-state]
    (try
      (reset! demo/app-state {:channels {"general" {:id "general"
                                                    :name "general"
                                                    :messages []}}
                              :channel-order ["general"]
                              :tab-state {}})
      (let [response (#'demo/send-message-handler
                      {:body (java.io.ByteArrayInputStream.
                              (.getBytes (json/write-str {:displayName "Alice"
                                                          :messageText "hello"
                                                          :channelId "general"})
                                         "UTF-8"))
                       :biff.datastar/tab-state {:channel-id "general"}})]
        (is (= 200 (:status response)))
        (is (= {:messageText ""} (json/read-str (:body response) :key-fn keyword)))
        (is (= 1 (count (get-in @demo/app-state [:channels "general" :messages])))))
      (finally
        (reset! demo/app-state previous-state)))))
