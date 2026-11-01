;;; fake-bridge.el --- Minimal fake zk-bridge for tests  -*- lexical-binding: t; -*-

;;; Commentary:

;; Run as: emacs -Q --batch -l fake-bridge.el
;; Speaks newline-delimited JSON-RPC on stdin/stdout with an in-memory
;; tree.  Responses are deliberately written in odd chunks.

;;; Code:

(require 'json)

(defvar fake-nodes (make-hash-table :test 'equal))
(puthash "/" (list :data "" :version 0) fake-nodes)
(puthash "/zookeeper" (list :data "" :version 0) fake-nodes)

(defun fake-out (obj)
  "Write OBJ as one line, split in two chunks."
  (let* ((s (concat (json-serialize obj :null-object :null :false-object :false) "\n"))
         (half (/ (length s) 2)))
    (princ (substring s 0 half))
    (princ (substring s half))))

(defun fake-stat (path)
  "Stat for PATH."
  (let ((n (gethash path fake-nodes)))
    (list :version (plist-get n :version) :czxid 1 :mzxid 1 :pzxid 1 :ctime 0 :mtime 0
          :cversion 0 :aversion 0 :ephemeral_owner 0
          :data_length (length (plist-get n :data))
          :num_children (length (fake-children path)))))

(defun fake-children (path)
  "Child names of PATH."
  (let (res)
    (maphash (lambda (k _)
               (when (and (not (equal k "/"))
                          (equal (if (string-match "\\`\\(.*\\)/[^/]+\\'" k)
                                     (let ((p (match-string 1 k))) (if (equal p "") "/" p))
                                   "")
                                 path))
                 (push (file-name-nondirectory k) res)))
             fake-nodes)
    (sort res #'string<)))

(defun fake-err (id kind msg)
  "Reply with ZkError."
  (fake-out (list :jsonrpc "2.0" :id id
                  :error (list :code -32001 :message msg
                               :data (list :kind kind :message msg)))))

(defun fake-handle (msg)
  "Handle MSG."
  (let* ((id (plist-get msg :id))
         (m (plist-get msg :method))
         (p (plist-get msg :params))
         (path (plist-get p :path))
         (ok (lambda (r) (fake-out (list :jsonrpc "2.0" :id id :result (or r :null))))))
    (pcase m
      ("initialize" (funcall ok (list :server "fake" :server_version "0" :protocol_version "1.0")))
      ("shutdown" (funcall ok nil) (kill-emacs 0))
      ("$/cancelRequest" nil)
      ("cluster.connect"
       ;; Also emit a notification before the response.
       (fake-out (list :jsonrpc "2.0" :method "connection.event"
                       :params (list :cluster "c1" :state "connected"
                                     :event (list :kind "connected" :generation 1 :at_ms 0))))
       (funcall ok (list :cluster "c1" :state "connected")))
      ("cluster.disconnect" (funcall ok nil))
      ("node.get"
       (if-let* ((n (gethash path fake-nodes)))
           (funcall ok (list :path path :stat (fake-stat path)
                             :data (base64-encode-string (plist-get n :data) t)
                             :utf8 t :truncated :false))
         (fake-err id "no-node" "no node")))
      ("node.children"
       (funcall ok (list :path path :stat (fake-stat path)
                         :total (length (fake-children path))
                         :children (vconcat
                                    (mapcar (lambda (c)
                                              (let ((cp (if (equal path "/") (concat "/" c)
                                                          (concat path "/" c))))
                                                (list :name c :path cp :stat (fake-stat cp))))
                                            (fake-children path)))
                         :truncated :false)))
      ("node.create"
       (if (gethash path fake-nodes) (fake-err id "node-exists" "exists")
         (puthash path (list :data (or (plist-get p :text) "") :version 0) fake-nodes)
         (funcall ok (list :path path :stat (fake-stat path)))))
      ("node.set"
       (let ((n (gethash path fake-nodes)))
         (cond ((null n) (fake-err id "no-node" "no node"))
               ((and (plist-get p :version) (/= (plist-get p :version) (plist-get n :version)))
                (fake-err id "bad-version" "version mismatch"))
               (t (puthash path (list :data (base64-decode-string (plist-get p :data))
                                      :version (1+ (plist-get n :version)))
                           fake-nodes)
                  (funcall ok (list :path path :stat (fake-stat path)))))))
      ("watch.add" (funcall ok (list :watch "w1")))
      ("watch.remove" (funcall ok nil))
      ("test.sleep" nil)                ; never answers
      (_ (fake-out (list :jsonrpc "2.0" :id id
                         :error (list :code -32601 :message "Method not found")))))))

(let (line)
  (while (setq line (ignore-errors (read-from-minibuffer "")))
    (unless (string-empty-p line)
      (fake-handle (json-parse-string line :object-type 'plist :array-type 'list
                                      :null-object nil :false-object :false)))))

;;; fake-bridge.el ends here
