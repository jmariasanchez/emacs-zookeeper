;;; zookeeper-integration-test.el --- End-to-end tests with a real bridge  -*- lexical-binding: t; -*-

;;; Commentary:

;; Requires ZK_BRIDGE (path to zk-bridge) and ZK_CONNECT (host:port).
;; Skipped otherwise.

;;; Code:

(require 'ert)
(require 'zookeeper)

(defun zookeeper-it--wait (pred &optional secs)
  "Wait up to SECS for PRED to return non-nil."
  (let ((deadline (+ (float-time) (or secs 10))))
    (while (and (not (funcall pred)) (< (float-time) deadline))
      (accept-process-output nil 0.05))
    (funcall pred)))

(ert-deftest zookeeper-integration-flow ()
  (let ((bridge (getenv "ZK_BRIDGE")) (connect (getenv "ZK_CONNECT")))
    (unless (and bridge connect (file-executable-p bridge))
      (ert-skip "ZK_BRIDGE and ZK_CONNECT not set"))
    (let* ((zookeeper-bridge-executable bridge)
           (zookeeper-bridge-auto-restart nil)
           (zookeeper-clusters `(("it" :connect ,connect :protected-paths ("/zookeeper/**"))))
           (zookeeper--connections (make-hash-table :test 'equal))
           (zookeeper-bridge--current nil)
           (root (format "/zkel-it-%d" (random 1000000)))
           (watch-events nil)
           (zookeeper-watch-event-functions
            (cons (lambda (p) (push p watch-events)) zookeeper-watch-event-functions)))
      (unwind-protect
          (progn
            ;; connect
            (should (stringp (zookeeper-connect "it")))
            (should (member (zookeeper-cluster-state "it") '("connected" "reconnected")))
            ;; create
            (zookeeper-call "it" "node.create" :path root :text "root")
            (zookeeper-call "it" "node.create" :path (concat root "/a/b") :text "{\"k\":1}"
                            :create_parents t)
            (let ((err (should-error (zookeeper-call "it" "node.create" :path root :text "x")
                                     :type 'zookeeper-error)))
              (should (equal (zookeeper-error-kind err) "node-exists")))
            ;; watch
            (let ((w (plist-get (zookeeper-call "it" "watch.add" :path root :kind "persistent")
                                :watch)))
              (should w)
              ;; browse children
              (let ((r (zookeeper-call "it" "node.children" :path root :stats t :limit 10)))
                (should (equal (mapcar (lambda (c) (plist-get c :name)) (plist-get r :children))
                               '("a")))
                (should (= 1 (plist-get r :total))))
              (let ((zookeeper-browser-live-refresh nil))
                (with-current-buffer (zookeeper-browse "it" (concat root "/a"))
                  (should (string-match-p "a" (buffer-string)))
                  (should (equal (zookeeper-browser-path-at-point) (concat root "/a")))
                  (kill-buffer)))
              ;; get / set with version conflict
              (let* ((g (zookeeper-call "it" "node.get" :path root))
                     (v (plist-get (plist-get g :stat) :version)))
                (should (equal (zookeeper-data-decode (plist-get g :data)) "root"))
                (zookeeper-call "it" "node.set" :path root :text "v2" :version v)
                (let ((err (should-error (zookeeper-call "it" "node.set" :path root
                                                         :text "v3" :version v)
                                         :type 'zookeeper-error)))
                  (should (equal (zookeeper-error-kind err) "bad-version"))))
              ;; watch event arrives
              (should (zookeeper-it--wait
                       (lambda () (cl-some (lambda (p) (equal (plist-get p :watch) w))
                                           watch-events))))
              ;; node buffer conflict path: external change, then save -> conflict
              (with-current-buffer (zookeeper-node-open "it" root)
                (zookeeper-call "it" "node.set" :path root :text "external")
                (goto-char (point-max)) (insert "!")
                (let (asked)
                  (cl-letf (((symbol-function 'read-multiple-choice)
                             (lambda (&rest _) (setq asked t) '(?r "reload"))))
                    (zookeeper-node-save))
                  (should asked))
                (should (string-match-p "external" (buffer-string)))
                (kill-buffer))
              ;; recursive delete: dry run, then execute
              (let ((dry (zookeeper-call "it" "node.delete" :path root :recursive t :dry_run t)))
                (should (zookeeper-true-p (plist-get dry :dry_run)))
                (should (= 3 (length (plist-get dry :paths))))
                (should (plist-get (zookeeper-call "it" "node.exists" :path root) :stat)))
              (zookeeper-call "it" "node.delete" :path root :recursive t)
              (should-not (plist-get (zookeeper-call "it" "node.exists" :path root) :stat))
              (zookeeper-rpc "watch.remove" (list :watch w)))
            ;; disconnect
            (zookeeper-disconnect "it")
            (should-not (zookeeper-connection "it")))
        (ignore-errors (when (zookeeper-connected-p "it")
                         (zookeeper-call "it" "node.delete" :path root :recursive t)))
        (zookeeper-bridge-stop)))))

(provide 'zookeeper-integration-test)
;;; zookeeper-integration-test.el ends here
