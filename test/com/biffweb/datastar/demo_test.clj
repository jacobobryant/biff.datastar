(ns com.biffweb.datastar.demo-test
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.test :refer [deftest is]]
    [com.biffweb.datastar.demo :as demo]))

(deftest demo-page-uses-published-datastar-bundle
  (let [response (demo/app-sync {:request-method :get
                                 :uri "/"
                                 :headers {}
                                 :query-params {}})
        body (:body response)]
    (is (= 200 (:status response)))
    (is (str/includes? body demo/datastar-script-url))
    (is (str/includes? body "const methods = ['get', 'post', 'put', 'patch', 'delete'];"))
    (is (str/includes? body "'X-CSRF-Token': "))
    (is (str/includes? body "data-signals:tab-id"))
    (is (str/includes? body "data-bind:channelId"))
    (is (str/includes? body "data-signals:message-count"))
    (is (str/includes? body "requestAnimationFrame(() =&gt; { el.scrollTop = el.scrollHeight; })"))
    (is (str/includes? body "@post(&quot;/messages&quot;)"))
    (is (not (str/includes? body "\"X-CSRF-Token\":")))
    (is (not (str/includes? body "{contentType: &apos;form&apos;")))))

(deftest send-message-action-clears-message-signal
  (let [previous-state @demo/app-state]
    (try
       (reset! demo/app-state {:channels {"general" {:id "general"
                                                     :name "general"
                                                     :messages []}}
                               :channel-order ["general"]
                               :tab-state {}})
        (let [response (#'demo/send-message-handler
                        {:biff.datastar/signals {:displayname "Alice"
                                                 :messagetext "hello"}
                          :biff.datastar/tab-state {:channel-id "general"}})]
        (is (= 200 (:status response)))
        (is (= "text/event-stream; charset=utf-8"
               (get-in response [:headers "Content-Type"])))
        (is (str/includes? (:body response) "event: datastar-patch-signals"))
        (is (str/includes? (:body response) (json/write-str {"messagetext" ""})))
        (is (= 1 (count (get-in @demo/app-state [:channels "general" :messages])))))
       (finally
         (reset! demo/app-state previous-state)))))

(deftest set-channel-action-uses-empty-204-response
  (let [response (#'demo/set-channel-handler
                   {:biff.datastar/signals {:channelid "general"}
                    :biff.datastar/tab-state {:channel-id "__new__"}})]
    (is (= 204 (:status response)))
    (is (nil? (:body response)))
    (is (= {:channel-id "general"}
           (:biff.datastar/tab-state response)))))
