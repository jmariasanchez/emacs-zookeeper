;;; zookeeper-test.el --- Unit tests for zookeeper.el  -*- lexical-binding: t; -*-

;;; Commentary:

;; Pure-function tests plus an end-to-end run against fake-bridge.el.

;;; Code:

(require 'ert)
(require 'zookeeper)

(defconst zookeeper-test--dir
  (file-name-directory (or load-file-name buffer-file-name)))

;;;; Framing and dispatch

(ert-deftest zookeeper-test-split-lines ()
  (should (equal (zookeeper-bridge--split-lines "" "{\"a\":1}\n{\"b\"")
                 '(("{\"a\":1}") . "{\"b\"")))
  (should (equal (zookeeper-bridge--split-lines "{\"b\"" ":2}\n\n{\"c\":3}\n")
                 '(("{\"b\":2}" "{\"c\":3}") . "")))
  (should (equal (zookeeper-bridge--split-lines "" "partial") '(nil . "partial"))))

(ert-deftest zookeeper-test-filter-partial-and-multiple ()
  (let* ((b (zookeeper-bridge--make))
         (proc (make-pipe-process :name "zk-test-dummy" :noquery t))
         results notes)
    (unwind-protect
        (progn
          (process-put proc 'zookeeper-bridge b)
          (puthash 1 (cons (lambda (r) (push r results)) #'ignore) (zookeeper-bridge-pending b))
          (puthash 2 (cons #'ignore (lambda (e) (push e results))) (zookeeper-bridge-pending b))
          (let ((zookeeper-audit-functions (list (lambda (p) (push p notes)))))
            (zookeeper-bridge--filter proc "{\"jsonrpc\":\"2.0\",\"id\":1,\"res")
            (should (null results))
            (zookeeper-bridge--filter
             proc (concat "ult\":{\"x\":[1,2]}}\n"
                          "{\"jsonrpc\":\"2.0\",\"method\":\"audit.record\",\"params\":{\"operation\":\"set\"}}\n"
                          "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-32001,\"message\":\"m\","
                          "\"data\":{\"kind\":\"bad-version\",\"message\":\"m\",\"path\":\"/a\"}}}\n")))
          (should (equal (car (last results)) '(:x (1 2))))
          (should (equal (zookeeper-error-kind (car results)) "bad-version"))
          (should (equal (plist-get (car notes) :operation) "set"))
          (should (= 0 (hash-table-count (zookeeper-bridge-pending b)))))
      (delete-process proc))))

(ert-deftest zookeeper-test-error-conversion ()
  (let ((e (zookeeper-bridge--rpc-error-to-plist
            '(:code -32001 :message "x" :data (:kind "no-node" :message "x" :path "/p" :op_index 2)))))
    (should (equal (plist-get e :kind) "no-node"))
    (should (equal (plist-get e :op_index) 2)))
  (should (equal (plist-get (zookeeper-bridge--rpc-error-to-plist '(:code -32601 :message "nf")) :kind)
                 "method-not-found"))
  (should (equal (plist-get (zookeeper-bridge--rpc-error-to-plist '(:code -32800 :message "c")) :kind)
                 "cancelled"))
  (let ((err (should-error (zookeeper-signal '(:kind "bad-version" :message "m" :path "/a"))
                           :type 'zookeeper-error)))
    (should (equal (zookeeper-error-kind err) "bad-version"))
    (should (equal (zookeeper-error-path err) "/a"))))

(ert-deftest zookeeper-test-version-check ()
  (should-not (zookeeper-bridge-check-version "1.3"))
  (should-error (zookeeper-bridge-check-version "2.0") :type 'zookeeper-error))

;;;; ACL

(ert-deftest zookeeper-test-acl-perms ()
  (should (= (zookeeper-acl-perms-to-int "cdrwa") 31))
  (should (equal (zookeeper-acl-perms-to-string 31) "cdrwa"))
  (should (equal (zookeeper-acl-perms-to-string "awr") "rwa"))
  (should (equal (zookeeper-acl-perms-to-string 0) ""))
  (should (equal (zookeeper-acl-toggle-perm "rw" ?a) "rwa"))
  (should (equal (zookeeper-acl-toggle-perm "rwa" ?w) "ra"))
  (should-error (zookeeper-acl-perms-to-int "rx"))
  (should (equal (zookeeper-acl-parse-entry "digest:bob:abc=:cr")
                 '(:scheme "digest" :id "bob:abc=" :perms "cr")))
  (should (equal (zookeeper-acl-format-entry '(:scheme "world" :id "anyone" :perms 31))
                 "world:anyone:cdrwa")))

;;;; Data views

(ert-deftest zookeeper-test-data-views ()
  (let ((bytes (encode-coding-string "héllo {\"a\":1}" 'utf-8)))
    (dolist (view '(text hex base64))
      (should (equal (zookeeper-data-parse (zookeeper-data-render bytes view) view) bytes))))
  (let ((bin (unibyte-string 0 255 16 32 200 1 2 3 4 5 6 7 8 9 10 11 12 13)))
    (should-not (zookeeper-data-utf8-p bin))
    (should (eq (zookeeper-data-auto-view bin) 'hex))
    (should (equal (zookeeper-data-unhex (zookeeper-data-hex bin)) bin))
    (should (equal (zookeeper-data-decode (zookeeper-data-encode bin)) bin)))
  (should (eq (zookeeper-data-auto-view "{\"a\": [1, 2]}") 'json))
  (should (eq (zookeeper-data-auto-view "plain") 'text))
  (should (string-match-p "\n" (zookeeper-data-render "{\"a\":1,\"b\":2}" 'json)))
  (should-error (zookeeper-data-parse "{bad" 'json) :type 'user-error))

;;;; Config conversion and secrets

(ert-deftest zookeeper-test-config-conversion ()
  (let ((zookeeper-clusters
         '(("prod" :connect "zk1:2181" :chroot "/app" :read-only t
            :protected-paths ("/kafka/**") :session-timeout-ms 30000
            :auth ((:scheme "digest" :credential (:command ("pass" "show" "zk")))
                   (:credential (:env "ZKD")))))))
    (let* ((cfg (zookeeper-profile-to-config "prod"))
           (json (zookeeper-json-parse (zookeeper-json-encode cfg))))
      (should (equal (plist-get json :connect) "zk1:2181"))
      (should (equal (plist-get json :chroot) "/app"))
      (should (equal (plist-get json :session_timeout_ms) 30000))
      (should (eq (plist-get (plist-get json :safety) :read_only) t))
      (should (equal (plist-get (plist-get json :safety) :protected_paths) '("/kafka/**")))
      (should (equal (plist-get (car (plist-get json :auth)) :credential)
                     '(:command ("pass" "show" "zk"))))
      (should (equal (plist-get (cadr (plist-get json :auth)) :credential) '(:env "ZKD"))))))

(ert-deftest zookeeper-test-auth-source-only-at-connect ()
  (let ((zookeeper-clusters
         '(("s" :connect "h:1" :auth ((:credential (:auth-source (:host "zk-s")))))))
        (looked-up 0))
    (cl-letf (((symbol-function 'zookeeper--auth-source-lookup)
               (lambda (_spec) (cl-incf looked-up) "u:SECRETPW")))
      ;; Not resolved for display.
      (let ((cfg (zookeeper-profile-to-config "s")))
        (should (= looked-up 0))
        (should-not (string-match-p "SECRETPW" (zookeeper-json-encode cfg))))
      ;; Resolved for connect, passed as value.
      (let ((cfg (zookeeper-profile-to-config "s" t)))
        (should (= looked-up 1))
        (should (equal (plist-get (plist-get (aref (plist-get cfg :auth) 0) :credential) :value)
                       "u:SECRETPW"))))))

;;;; Browser rendering and marks

(defun zookeeper-test--browser ()
  "Return a browser buffer filled with canned data."
  (let ((buf (generate-new-buffer "*zk-test-browser*")))
    (with-current-buffer buf
      (let ((zookeeper-browser-live-refresh nil))
        (zookeeper-browser-mode))
      (setq zookeeper-cluster "t")
      (puthash "/" t zookeeper-browser--expanded)
      (puthash "/" (list :total 3 :stat '(:num_children 3)
                         :children (list (list :name "app" :path "/app"
                                               :stat '(:num_children 2 :ephemeral_owner 0 :data_length 0))
                                         (list :name "lock-0000000001" :path "/lock-0000000001"
                                               :stat '(:num_children 0 :ephemeral_owner 4096 :data_length 3))
                                         (list :name "zookeeper" :path "/zookeeper"
                                               :stat '(:num_children 0 :ephemeral_owner 0))))
               zookeeper-browser--children)
      (puthash "/app" t zookeeper-browser--expanded)
      (puthash "/app" (list :total 1200 :children (list (list :name "c1" :path "/app/c1"
                                                               :stat '(:num_children 0))))
               zookeeper-browser--children)
      (zookeeper-browser-render))
    buf))

(ert-deftest zookeeper-test-tree-rendering ()
  (let ((zookeeper-clusters '(("t" :connect "x" :protected-paths ("/zookeeper/**")))))
    (with-current-buffer (zookeeper-test--browser)
      (unwind-protect
          (let ((text (buffer-string)))
            (should (string-match-p "▾ app  (2)" text))
            (should (string-match-p "    · c1" text))
            (should (string-match-p "more\\.\\.\\. (1 of 1200 shown" text))
            (should (string-match-p "lock-0000000001 ⚡" text))
            (should (equal (zookeeper-browser-visible-paths)
                           '("/" "/app" "/app/c1" "/lock-0000000001" "/zookeeper")))
            (zookeeper-browser--goto-line-of "/lock-0000000001")
            (should (eq (get-text-property (+ (point) 6) 'face) 'zookeeper-ephemeral))
            (zookeeper-browser--goto-line-of "/zookeeper")
            (should (eq (get-text-property (+ (point) 6) 'face) 'zookeeper-protected)))
        (kill-buffer)))))

(ert-deftest zookeeper-test-marks ()
  (let ((zookeeper-clusters '(("t" :connect "x"))))
    (with-current-buffer (zookeeper-test--browser)
      (unwind-protect
          (progn
            (zookeeper-browser--goto-line-of "/app")
            (zookeeper-browser-mark)
            (should (equal (zookeeper-browser-path-at-point) "/app/c1"))
            (zookeeper-browser-flag-deletion)
            (should (equal (zookeeper-browser-marked) '("/app")))
            (should (equal (zookeeper-browser-marked ?D) '("/app/c1")))
            (should (string-match-p "^\\* " (buffer-string)))
            (should (string-match-p "^D " (buffer-string)))
            (zookeeper-browser-mark-regexp "lock")
            (should (equal (zookeeper-browser-marked) '("/app" "/lock-0000000001")))
            (should (equal (zookeeper-browser-targets) '("/app" "/lock-0000000001")))
            (zookeeper-browser--goto-line-of "/app")
            (zookeeper-browser-unmark)
            (should (equal (zookeeper-browser-marked) '("/lock-0000000001")))
            (zookeeper-browser-unmark-all)
            (should (null (zookeeper-browser-marked ?D))))
        (kill-buffer)))))

(ert-deftest zookeeper-test-glob ()
  (should (zookeeper-glob-match-p "/kafka/**" "/kafka"))
  (should (zookeeper-glob-match-p "/kafka/**" "/kafka/a/b"))
  (should-not (zookeeper-glob-match-p "/kafka/**" "/kafkaX"))
  (should (zookeeper-glob-match-p "/a/*/c" "/a/b/c"))
  (should-not (zookeeper-glob-match-p "/a/*/c" "/a/b/x/c")))

(ert-deftest zookeeper-test-paths-and-format ()
  (should (equal (zookeeper-path-join "/" "a") "/a"))
  (should (equal (zookeeper-path-join "/a" "b") "/a/b"))
  (should (equal (zookeeper-path-parent "/a/b") "/a"))
  (should (equal (zookeeper-path-parent "/a") "/"))
  (should (equal (zookeeper-format-session 72057594037927936) "0x100000000000000"))
  (should-not (zookeeper-format-session 0))
  (should (string-match-p "only-left +x" (zookeeper-diff-format
                                          '(:differences ((:rel "x" :kind "only-left")))
                                          "a" "/" "b" "/"))))

(ert-deftest zookeeper-test-events-log ()
  (let ((zookeeper-events nil))
    (zookeeper-events-record 'audit '(:cluster "p" :operation "node.set" :paths ("/a")
                                      :outcome "ok" :forced t))
    (zookeeper-events-record 'watch '(:watch "w1" :cluster "c9"
                                      :event (:kind "node-created" :path "/b")))
    (should (= 2 (length zookeeper-events)))
    (let ((zookeeper-events-filter "node-created"))
      (should (equal (plist-get (car (zookeeper-events-visible)) :path) "/b")))
    (should (string-match-p "FORCED" (plist-get (car (last zookeeper-events)) :detail)))))

;;;; End-to-end with the fake bridge

(defmacro zookeeper-test--with-fake (&rest body)
  "Run BODY with a fake bridge running."
  (declare (indent 0))
  `(let ((zookeeper-bridge-executable (expand-file-name invocation-name invocation-directory))
         (zookeeper-bridge-arguments
          (list "-Q" "--batch" "-l" (expand-file-name "fake-bridge.el" zookeeper-test--dir)))
         (zookeeper-bridge-auto-restart nil)
         (zookeeper-clusters '(("fake" :connect "fake:1")))
         (zookeeper--connections (make-hash-table :test 'equal))
         (zookeeper-bridge--current nil))
     (unwind-protect (progn (zookeeper-bridge-start) ,@body)
       (zookeeper-bridge-stop))))

(ert-deftest zookeeper-test-fake-end-to-end ()
  (zookeeper-test--with-fake
    (let ((events nil))
      (let ((zookeeper-connection-event-functions
             (cons (lambda (p) (push p events)) zookeeper-connection-event-functions)))
        (should (equal (zookeeper-connect "fake") "c1")))
      (should events)
      (should (equal (zookeeper-cluster-state "fake") "connected"))
      (zookeeper-call "fake" "node.create" :path "/x" :text "hi")
      (let ((r (zookeeper-call "fake" "node.children" :path "/" :stats t)))
        (should (member "x" (mapcar (lambda (c) (plist-get c :name)) (plist-get r :children)))))
      (should (= 1 (plist-get (plist-get (zookeeper-call "fake" "node.set" :path "/x"
                                                         :data (zookeeper-data-encode "a")
                                                         :version 0)
                                         :stat)
                              :version)))
      (let ((err (should-error (zookeeper-call "fake" "node.set" :path "/x"
                                               :data (zookeeper-data-encode "b") :version 0)
                               :type 'zookeeper-error)))
        (should (equal (zookeeper-error-kind err) "bad-version")))
      (let ((err (should-error (zookeeper-rpc "recipe.inspect" '(:path "/")) :type 'zookeeper-error)))
        (should (equal (zookeeper-error-kind err) "method-not-found")))
      (should-error (zookeeper-recipe-inspect "fake" "/" "auto") :type 'user-error)
      (let ((err (should-error (zookeeper-rpc "test.sleep" '(:a 1) 0.3) :type 'zookeeper-error)))
        (should (equal (zookeeper-error-kind err) "timeout")))
      ;; Browser and node buffer against the fake.
      (let ((zookeeper-browser-live-refresh nil))
        (with-current-buffer (zookeeper-browse "fake")
          (should (string-match-p "x" (buffer-string)))
          (kill-buffer)))
      (with-current-buffer (zookeeper-node-open "fake" "/x")
        (should (string-match-p "version 1" (buffer-string)))
        (goto-char (point-max))
        (insert "Z")
        (cl-letf (((symbol-function 'read-multiple-choice) (lambda (&rest _) '(?q "quit"))))
          (zookeeper-node-save))
        (should (string-match-p "version 2" (buffer-string)))
        (kill-buffer)))))

(ert-deftest zookeeper-test-quit-cancels ()
  (zookeeper-test--with-fake
    (let ((sent nil))
      (cl-letf* ((orig (symbol-function 'zookeeper-rpc-cancel))
                 ((symbol-function 'zookeeper-rpc-cancel)
                  (lambda (id) (push id sent) (funcall orig id))))
        (run-at-time 0.2 nil (lambda () (setq quit-flag t)))
        (should (condition-case nil (progn (zookeeper-rpc "test.sleep" nil 5) nil)
                  (quit t)))
        (should sent)
        (should (= 0 (hash-table-count (zookeeper-bridge-pending zookeeper-bridge--current))))))))

(ert-deftest zookeeper-test-cache-fallback-to-watch ()
  (zookeeper-test--with-fake
    (let ((zookeeper-browser--cache-unsupported nil)
          (zookeeper-browser-live-refresh t))
      (with-current-buffer (zookeeper-browse "fake")
        (unwind-protect
            (progn
              (should (zookeeper-it-wait-for
                       (lambda () (equal (gethash "/" zookeeper-browser--watches) "w1"))))
              (should zookeeper-browser--cache-unsupported)
              (should (equal (zookeeper-browser--header-line) " live refresh: watches")))
          (kill-buffer))))))

(defun zookeeper-it-wait-for (pred &optional secs)
  "Wait up to SECS for PRED."
  (let ((deadline (+ (float-time) (or secs 5))))
    (while (and (not (funcall pred)) (< (float-time) deadline))
      (accept-process-output nil 0.05))
    (funcall pred)))

(ert-deftest zookeeper-test-browser-freshness ()
  (with-temp-buffer
    (setq zookeeper-browser--cache-states (make-hash-table :test 'equal))
    (should-not (zookeeper-browser-freshness))
    (puthash "k1" "live" zookeeper-browser--cache-states)
    (should (equal (zookeeper-browser-freshness) "live"))
    (should-not (string-match-p "STALE" (zookeeper-browser--header-line)))
    (puthash "k2" "stale" zookeeper-browser--cache-states)
    (should (equal (zookeeper-browser-freshness) "stale"))
    (should (string-match-p "STALE" (zookeeper-browser--header-line)))))

(ert-deftest zookeeper-test-follow-never-clobbers ()
  (zookeeper-test--with-fake
    (zookeeper-call "fake" "node.create" :path "/f" :text "one")
    (with-current-buffer (zookeeper-node-open "fake" "/f")
      (unwind-protect
          (progn
            ;; Simulate follow mode without a real cache.
            (setq zookeeper-node-follow-mode t zookeeper-node--cache "k9")
            (zookeeper-call "fake" "node.set" :path "/f" :data (zookeeper-data-encode "two"))
            (zookeeper-node--on-cache-event '(:cache "k9" :event (:type "node-updated" :path "/f")))
            (should (string-match-p "two" (buffer-string)))
            (goto-char (point-max)) (insert "LOCAL")
            (zookeeper-call "fake" "node.set" :path "/f" :data (zookeeper-data-encode "three"))
            (zookeeper-node--on-cache-event '(:cache "k9" :event (:type "node-updated" :path "/f")))
            (should (string-match-p "twoLOCAL" (buffer-string)))
            (should zookeeper-node--server-changed)
            (should (string-match-p "C-c C-r" (zookeeper-node--follow-header))))
        (setq zookeeper-node--cache nil)
        (set-buffer-modified-p nil)
        (kill-buffer)))))

(ert-deftest zookeeper-test-recipe-render ()
  (let ((lock (zookeeper-recipe-render
               '(:kind "lock" :path "/l" :owner "n-0000000001"
                 :contenders ((:node "n-0000000002" :sequence 2)
                              (:node "n-0000000001" :sequence 1 :is_owner t :session_id "0x1"))))))
    (should (string-match-p "Owner: n-0000000001" lock))
    (should (string-match-p "Contenders (2)" lock))
    (should (< (string-match "#1" lock) (string-match "#2" lock)))
    (should (string-match-p "★ #1 +n-0000000001  session 0x1  OWNER" lock)))
  (should (string-match-p "Size: 3   Ready: 2"
                          (zookeeper-recipe-render
                           '(:kind "queue" :path "/q" :size 3 :ready 2
                             :head ((:node "qn-0000000000" :sequence 0))))))
  (should (string-match-p "int64: +5"
                          (zookeeper-recipe-render '(:kind "counter" :path "/c" :int64 5))))
  (should (string-match-p "Leader: none"
                          (zookeeper-recipe-render '(:kind "leader-latch" :path "/x"))))
  (should (string-match-p "svc  (1 instance)\n +i1  10.0.0.1:80"
                          (zookeeper-recipe-render
                           '(:kind "service-registry" :path "/s"
                             :services ((:name "svc" :instances ((:id "i1" :address "10.0.0.1" :port 80))))))))
  (should (string-match-p "No known recipe"
                          (zookeeper-recipe-render '(:kind "unknown" :path "/u" :children 0)))))

(ert-deftest zookeeper-test-recipe-ownership-warning ()
  (let ((zookeeper-recipe--handles (make-hash-table :test 'equal)))
    (zookeeper-recipe--register '(:recipe "r1" :node "/l/n" :ownership "held") "lock" "c" "/l")
    (should-not (zookeeper-recipes--warning))
    (zookeeper-recipe--on-ownership '(:recipe "r1" :state "lost"))
    (should (equal (zookeeper-recipe-state "r1") "lost"))
    (should (string-match-p "NOT HELD: r1 lost" (zookeeper-recipes--warning)))
    (zookeeper-recipe--on-ownership '(:recipe "r1" :state "released"))
    (should-not (zookeeper-recipes--warning))
    (zookeeper-recipes-forget-released)
    (should (= 0 (hash-table-count zookeeper-recipe--handles)))))

(provide 'zookeeper-test)
;;; zookeeper-test.el ends here
