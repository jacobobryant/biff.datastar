(ns com.biffweb.datastar.demo-test
  (:require
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
    (is (str/includes? body demo/datastar-script-url))))
