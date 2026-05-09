(ns com.biffweb.datastar.demo
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [com.biffweb.datastar :as datastar]
   [dev.onionpancakes.chassis.core :as chassis]
   [ring.middleware.anti-forgery :refer [wrap-anti-forgery]]
   [ring.middleware.params :refer [wrap-params]]
   [ring.middleware.session :refer [wrap-session]]
   [ring.util.jakarta.servlet :as servlet])
  (:import
    (java.io InputStream)
    (java.time Instant)
    (java.util UUID)
    (java.util.concurrent Executors)
    (jakarta.servlet.http HttpServletRequest HttpServletResponse)
    (org.eclipse.jetty.ee9.servlet ServletContextHandler ServletHolder)
    (org.eclipse.jetty.server Server ServerConnector)
    (org.eclipse.jetty.util.thread ExecutorThreadPool)))

(defonce app-state
  (atom {:channels {"general" {:id "general"
                               :name "general"
                               :messages []}}
         :channel-order ["general"]}))

(defonce lock-state
  (datastar/new-lock))

(def datastar-script-url
  "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js")

(def new-channel-option "__new__")

(add-watch app-state ::refresh
            (fn [_ _ old-state new-state]
              (when-not (= old-state new-state)
                (datastar/refresh lock-state))))

(defn- vthread-pool []
  (doto (ExecutorThreadPool.)
    (.setVirtualThreadsExecutor (Executors/newVirtualThreadPerTaskExecutor))))

(defn- csrf-headers-js [req]
  (str "{'X-CSRF-Token': "
       (pr-str (:anti-forgery-token req))
       ", 'X-Biff-Datastar-Tab-ID': $tabId}"))

(defn- action [method path req]
  (str "@"
       method
       "("
       (pr-str path)
       ", {headers: "
       (csrf-headers-js req)
       "})"))

(defn- update-url-js [signal-name]
  (str "window.history.replaceState("
       "null, '', '?channel=' + encodeURIComponent($" signal-name "));"))

(defn- parse-json-body [req]
  (if (instance? InputStream (:body req))
    (json/read-str (slurp (:body req)) :key-fn keyword)
    {}))

(defn- datastar-signals [req]
  (let [body (parse-json-body req)]
    (or (:datastar body) body {})))

(defn- trim-to-nil [s]
  (let [s (some-> s str str/trim)]
    (when (seq s) s)))

(defn- query-channel [req]
  (some-> (get-in req [:query-params "channel"])
          trim-to-nil))

(defn- selected-channel-id [channel-id]
  (when (and channel-id (not= channel-id new-channel-option))
    channel-id))

(defn- current-channel-id [req]
  (or (selected-channel-id (get-in req [:biff.datastar/tab-state :channel-id]))
      (selected-channel-id (query-channel req))
      "general"))

(defn- ensure-channel! [channel-id]
  (swap! app-state
         (fn [state]
           (if (get-in state [:channels channel-id])
             state
             (-> state
                 (assoc-in [:channels channel-id] {:id channel-id
                                                   :name channel-id
                                                   :messages []})
                 (update :channel-order conj channel-id))))))

(defn- channels []
  (mapv #(get-in @app-state [:channels %]) (:channel-order @app-state)))

(defn- channel-view [channel-id]
  (get-in @app-state [:channels channel-id]
          {:id channel-id :name channel-id :messages []}))

(defn- page-head [req]
  [:head
   [:meta {:charset "utf-8"}]
   [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
   [:meta {:name "csrf-token" :content (:anti-forgery-token req)}]
   [:script {:type "module"
             :src datastar-script-url}]
   [:title "biff.datastar demo"]
   [:style "
body { font-family: system-ui, sans-serif; margin: 0; background: #f5f7fb; color: #1f2937; }
.page { max-width: 960px; margin: 0 auto; padding: 2rem 1rem 3rem; }
.panel { background: white; border: 1px solid #dbe3ee; border-radius: 12px; padding: 1rem; box-shadow: 0 8px 30px rgba(15, 23, 42, 0.05); }
.stack { display: grid; gap: 1rem; }
.controls { display: grid; gap: 0.75rem; }
.row { display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center; }
label { display: grid; gap: 0.35rem; font-size: 0.95rem; font-weight: 600; }
input, select, textarea, button { font: inherit; }
input, select, textarea { width: 100%; padding: 0.7rem 0.85rem; border: 1px solid #cbd5e1; border-radius: 10px; background: white; }
textarea { min-height: 7rem; resize: vertical; }
button { border: 0; border-radius: 10px; background: #2563eb; color: white; padding: 0.75rem 1rem; cursor: pointer; }
button.secondary { background: #475569; }
.messages { height: 22rem; overflow-y: auto; border: 1px solid #dbe3ee; border-radius: 10px; padding: 0.75rem; background: #f8fafc; }
.message-list { display: grid; gap: 0.75rem; }
.message { background: white; border: 1px solid #dbe3ee; border-radius: 10px; padding: 0.75rem; }
.message header { display: flex; justify-content: space-between; gap: 1rem; font-size: 0.9rem; color: #475569; margin-bottom: 0.35rem; }
.message p { margin: 0; white-space: pre-wrap; }
.muted { color: #64748b; font-size: 0.95rem; }
.grow { flex: 1 1 18rem; }
" ]])

(defn- channel-selector [req selected-channel]
  [:div.stack {:data-signals:channel-id__ifmissing (pr-str selected-channel)
               :data-signals:new-channel-name__ifmissing "''"}
   [:label
    "Channel"
    [:select {:id "channel-select"
              :data-bind:channel-id ""
              :data-on:change (str "const showNewChannel = $channelId === '__new__';"
                                   "document.getElementById('new-channel-form').style.display = showNewChannel ? '' : 'none';"
                                   "if(!showNewChannel){"
                                   (action "post" "/channel" req)
                                   ";"
                                   (update-url-js "channelId")
                                   "}") }
      (for [{:keys [id name]} (channels)]
        [:option {:value id} name])
      [:option {:value new-channel-option} "new channel..."]]]
    [:div.stack {:id "new-channel-form"
                 :style "display:none"}
     [:label
      "Create a channel"
      [:input {:id "new-channel-name"
               :placeholder "team-updates"
               :data-bind:new-channel-name ""}]]
     [:div.row
      [:button {:type "button"
                :data-on:click (str "const channelId = $newChannelName.trim();"
                                    "if(channelId !== ''){"
                                    "$channelId = channelId;"
                                    (action "post" "/channels" req)
                                    ";"
                                    "window.history.replaceState(null, '', '?channel=' + encodeURIComponent(channelId));"
                                    "$newChannelName = '';"
                                    "document.getElementById('new-channel-form').style.display = 'none';"
                                    "}")} "Create channel"]]]])

(defn- message-list [selected-channel]
  (let [{:keys [messages name]} (channel-view selected-channel)]
    [:div.stack
     [:div.row
      [:h2 {:style "margin:0"} (str "#" name)]
      [:div.muted (str (count messages) " messages")]]
     [:div#messages.messages
      {:data-signals:messages-at-bottom__ifmissing "true"
       :data-on:scroll "$messagesAtBottom = el.scrollHeight - el.scrollTop - el.clientHeight <= 8"
       :data-effect "$messagesAtBottom && (el.scrollTop = el.scrollHeight)"}
      (if (seq messages)
        [:div.message-list
         (for [{:keys [id display-name text created-at]} messages]
           [:article.message {:id (str "message-" id)}
            [:header
             [:strong display-name]
             [:span (.toString ^Instant created-at)]]
            [:p text]])]
        [:p.muted "No messages yet. Say hello."])]]))

(defn- composer [req]
  [:div.stack {:data-signals:display-name__ifmissing (pr-str "Sprite")
               :data-signals:message-text__ifmissing "''"}
   [:label
    "Display name"
    [:input {:id "display-name"
              :placeholder "Sprite"
              :data-bind:display-name ""}]]
   [:label
    "Message"
    [:textarea {:id "message-text"
                 :placeholder "Type a message..."
                 :data-bind:message-text ""}]]
    [:div.row
     [:button {:type "button"
               :data-on:click (str "if($messageText.trim() !== ''){"
                                   (action "post" "/messages" req)
                                   ";"
                                   "$messageText = '';"
                                   "}")} "Send"]]])

(defn- content [req]
  (let [selected-channel (current-channel-id req)]
    [:div#biff_datastar_content.page
      [:div.stack
       [:div.panel.stack
        [:h1 {:style "margin:0"} "biff.datastar demo chat"]
       [:p.muted {:style "margin:0"} "One page, live updates, and per-tab channel state."]
       (channel-selector req selected-channel)]
      [:div.panel.stack
       (message-list selected-channel)]
      [:div.panel
       (composer req)]]]))

(defn- page-response [req]
  (let [channel-id (current-channel-id req)
        new-tab-state (assoc (or (:biff.datastar/tab-state req) {}) :channel-id channel-id)]
    (ensure-channel! channel-id)
    {:status 200
     :biff.datastar/tab-state new-tab-state
     :headers {"Content-Type" "text/html; charset=utf-8"}
     :body (if (:biff.datastar/sse-request req)
              (content req)
              (chassis/html
                [chassis/doctype-html5
                 [:html {:lang "en"}
                  (page-head req)
                  [:body
                   [:div (merge {:class "stack"} (datastar/container-opts req))]
                   (content req)]]]))}))

(defn- persist-tab-state! [req tab-state]
  (when-let [set-tab-state (:biff.datastar/set-tab-state req)]
    (set-tab-state req
                   (:biff.datastar/user-id req)
                   (:biff.datastar/tab-id req)
                   tab-state)))

(defn- channel-handler [req]
  (let [channel-id (selected-channel-id (trim-to-nil (:channelId (datastar-signals req))))
         new-tab-state (assoc (or (:biff.datastar/tab-state req) {}) :channel-id channel-id)]
    (when channel-id
      (persist-tab-state! req new-tab-state)
      (datastar/refresh req))
    {:status 204}))

(defn- create-channel-handler [req]
  (let [channel-id (trim-to-nil (:newChannelName (datastar-signals req)))]
    (if channel-id
      (do
        (persist-tab-state! req (assoc (or (:biff.datastar/tab-state req) {}) :channel-id channel-id))
        (ensure-channel! channel-id)
        (datastar/refresh req)
        {:status 204})
      {:status 204})))

(defn- send-message-handler [req]
  (let [{:keys [displayName messageText channelId]} (datastar-signals req)
        channel-id (or (selected-channel-id (trim-to-nil channelId))
                       (current-channel-id req))
        display-name (or (trim-to-nil displayName) "Anonymous")
        message-text (trim-to-nil messageText)]
    (if (and channel-id message-text)
      (do
        (ensure-channel! channel-id)
         (swap! app-state update-in [:channels channel-id :messages]
                conj {:id (str (UUID/randomUUID))
                      :display-name display-name
                      :text message-text
                      :created-at (Instant/now)})
         {:status 204})
       {:status 204})))

(defn- not-found [_]
  {:status 404
   :headers {"Content-Type" "text/plain; charset=utf-8"}
   :body "not found"})

(defn- wrap-demo-context [handler]
  (fn [req]
    (handler (merge req lock-state
                    {:biff.datastar/get-user-id (constantly "demo-user")
                     :biff.datastar/get-tab-state (fn [_ _ tab-id]
                                                    (get-in @app-state [:tab-state tab-id]))
                     :biff.datastar/set-tab-state (fn [_ _ tab-id tab-state]
                                                    (swap! app-state assoc-in [:tab-state tab-id] tab-state))}))))

(defn- routes [req]
  (case [(:request-method req) (:uri req)]
    [:get "/"] (page-response req)
    [:head "/"] (page-response req)
    [:post "/channel"] (channel-handler req)
    [:post "/channels"] (create-channel-handler req)
    [:post "/messages"] (send-message-handler req)
    (not-found req)))

(def app-sync
  (-> routes
      datastar/wrap-datastar
      wrap-demo-context
      wrap-anti-forgery
      wrap-session
      wrap-params))

(defn app
  ([req]
   (app-sync req))
  ([req respond raise]
   (future
     (try
       (let [response-map (app-sync req)]
         (if (and (:servlet-request req)
                  (instance? InputStream (:body response-map)))
           (let [servlet-request ^HttpServletRequest (:servlet-request req)
                 servlet-response ^HttpServletResponse (:servlet-response req)]
             (try
               (when-let [status (:status response-map)]
                 (.setStatus servlet-response status))
               (doseq [[header value] (:headers response-map)]
                 (if (string? value)
                   (.setHeader servlet-response header value)
                   (doseq [item value]
                     (.addHeader servlet-response header item))))
               (when-let [content-type (get-in response-map [:headers "Content-Type"])]
                 (.setContentType servlet-response content-type))
               (with-open [body ^InputStream (:body response-map)
                           out (.getOutputStream servlet-response)]
                 (let [buf (byte-array 8192)]
                   (loop []
                     (let [read (.read body buf)]
                       (when (pos? read)
                         (.write out buf 0 read)
                         (.flush out)
                         (recur))))))
               (catch Throwable t
                 (try
                   (.sendError servlet-response 500 (.getMessage t))
                   (catch Exception _))
                 (raise t))
               (finally
                 (.complete (.getAsyncContext servlet-request)))))
           (respond response-map)))
       (catch Throwable t
         (raise t))))))

(defonce server (atom nil))

(defn start! []
  (let [server* (Server. (vthread-pool))
        connector (doto (ServerConnector. server*)
                    (.setPort 8080))
        handler (doto (ServletContextHandler.)
                  (.setAllowNullPathInfo true)
                  (.addServlet (ServletHolder. (servlet/servlet #'app {:async? true})) "/*"))]
    (.addConnector server* connector)
    (.setHandler server* handler)
    (.start server*)
    (reset! server server*))
  @server)

(defn stop! []
  (when-let [server* @server]
    (.stop server*)
    (reset! server nil)))

(defn -main [& _]
  (start!)
  (println "Demo running on http://localhost:8080"))
