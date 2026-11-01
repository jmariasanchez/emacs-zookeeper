;;; zookeeper-bridge.el --- zk-bridge process and JSON-RPC transport  -*- lexical-binding: t; -*-

;; Copyright (C) 2026

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Process management and JSON-RPC 2.0 transport for the `zk-bridge'
;; helper.  Messages are newline-delimited JSON objects (see
;; docs/api/protocol.md).  JSON objects are represented as plists with
;; keyword keys, arrays as lists, JSON null as nil and JSON false as
;; `:false'.  When sending, arrays must be vectors.

;;; Code:

(require 'cl-lib)
(require 'subr-x)

(defgroup zookeeper-bridge nil
  "The zk-bridge helper process."
  :group 'zookeeper
  :prefix "zookeeper-bridge-")

(defcustom zookeeper-bridge-executable nil
  "Path or name of the zk-bridge executable.
When nil, it is searched in `exec-path', the package `bin/'
directory and the development tree `target/{release,debug}'."
  :type '(choice (const :tag "Auto-detect" nil) string))

(defcustom zookeeper-bridge-arguments nil
  "Extra command line arguments for zk-bridge."
  :type '(repeat string))

(defcustom zookeeper-bridge-remote nil
  "When non-nil, run the bridge on the host of a remote `default-directory'.
The process is started with `start-file-process', so it works with
TRAMP.  The value may be a remote directory name (for example
\"/ssh:jump:/tmp/\") or t to use `default-directory' as is."
  :type '(choice (const :tag "Local" nil)
                 (const :tag "Current remote directory" t)
                 (directory :tag "Remote directory")))

(defcustom zookeeper-bridge-log-level "warn"
  "Value of ZK_BRIDGE_LOG for the bridge process."
  :type '(choice (const "error") (const "warn") (const "info")
                 (const "debug") (const "trace")))

(defcustom zookeeper-bridge-timeout 30
  "Default timeout in seconds for synchronous RPC calls."
  :type 'number)

(defcustom zookeeper-bridge-auto-restart t
  "When non-nil, restart the bridge automatically after a crash."
  :type 'boolean)

(defconst zookeeper-bridge-protocol-version "1.0"
  "Protocol version implemented by this client.")

(defconst zookeeper-bridge-client-name "zookeeper.el 0.1"
  "Client name sent in `initialize'.")

(defconst zookeeper-bridge--source-dir
  (file-name-directory (or load-file-name buffer-file-name default-directory))
  "Directory containing this library.")

;;;; Hooks

(defvar zookeeper-connection-event-functions nil
  "Abnormal hook run with the params plist of each `connection.event'.")
(defvar zookeeper-watch-event-functions nil
  "Abnormal hook run with params of `watch.event' and `watch.removed'.")
(defvar zookeeper-cache-event-functions nil
  "Abnormal hook run with the params plist of each `cache.event'.")
(defvar zookeeper-audit-functions nil
  "Abnormal hook run with the params plist of each `audit.record'.")
(defvar zookeeper-recipe-ownership-functions nil
  "Abnormal hook run with the params plist of each `recipe.ownership'.")
(defvar zookeeper-notification-functions nil
  "Abnormal hook run with METHOD and PARAMS for every notification.")
(defvar zookeeper-bridge-restart-hook nil
  "Hook run after the bridge was (re)started and initialized.")

;;;; Errors

(define-error 'zookeeper-error "ZooKeeper error")

(defun zookeeper-error-data (err)
  "Return the ZkError plist carried by condition ERR."
  (if (eq (car-safe err) 'zookeeper-error) (cadr err) err))

(defun zookeeper-error-kind (err)
  "Return the kind string of ZooKeeper condition ERR."
  (plist-get (zookeeper-error-data err) :kind))

(defun zookeeper-error-message (err)
  "Return the human message of ZooKeeper condition ERR."
  (plist-get (zookeeper-error-data err) :message))

(defun zookeeper-error-path (err)
  "Return the path of ZooKeeper condition ERR, if any."
  (plist-get (zookeeper-error-data err) :path))

(defun zookeeper-error-op-index (err)
  "Return the transaction op index of ZooKeeper condition ERR, if any."
  (plist-get (zookeeper-error-data err) :op_index))

(defun zookeeper-bridge--rpc-error-to-plist (error)
  "Convert a JSON-RPC ERROR object plist into a ZkError plist."
  (let* ((code (plist-get error :code))
         (msg (plist-get error :message))
         (data (plist-get error :data)))
    (cond
     ((and (listp data) (plist-get data :kind))
      (append data (list :code code)))
     (t (list :kind (pcase code
                      (-32800 "cancelled")
                      (-32002 "unknown-handle")
                      (-32601 "method-not-found")
                      (-32602 "bad-arguments")
                      ((or -32700 -32600) "protocol")
                      (_ "other"))
              :message msg :code code)))))

(defun zookeeper-bridge--error-string (zkerr)
  "Format ZKERR plist as a short string."
  (concat (or (plist-get zkerr :kind) "error")
          (if-let* ((p (plist-get zkerr :path))) (format " [%s]" p) "")
          ": " (or (plist-get zkerr :message) "")))

(defun zookeeper-signal (zkerr)
  "Signal a `zookeeper-error' carrying ZKERR plist."
  (signal 'zookeeper-error (list zkerr)))

;; Make `error-message-string' readable.
(put 'zookeeper-error 'error-message "ZooKeeper error")

;;;; JSON helpers

(defun zookeeper-json-parse (string)
  "Parse JSON STRING into the plist representation."
  (json-parse-string string :object-type 'plist :array-type 'list
                     :null-object nil :false-object :false))

(defun zookeeper-json-encode (object)
  "Serialize OBJECT (plist / vector representation) to a JSON string."
  (json-serialize object :null-object :null :false-object :false))

(defun zookeeper-true-p (value)
  "Return non-nil if JSON VALUE is true."
  (and value (not (eq value :false))))

;;;; Framing

(defun zookeeper-bridge--split-lines (buffered chunk)
  "Append CHUNK to BUFFERED and split into complete lines.
Return a cons (LINES . REST) where LINES are complete lines without
the newline and REST is the partial trailing text."
  (let* ((s (concat buffered chunk))
         (parts (split-string s "\n"))
         (rest (car (last parts))))
    (cons (cl-remove-if #'string-empty-p (butlast parts)) rest)))

;;;; Process state

(cl-defstruct (zookeeper-bridge (:constructor zookeeper-bridge--make)
                                (:copier nil))
  process
  (next-id 0)
  (pending (make-hash-table :test 'eql))
  (partial "")
  info
  (stopping nil))

(defvar zookeeper-bridge--current nil
  "The running `zookeeper-bridge' object, or nil.")

(defvar zookeeper-bridge-log-buffer-name "*zk-bridge log*"
  "Name of the buffer receiving bridge stderr and protocol notes.")

(defvar zookeeper-bridge-trace nil
  "When non-nil, log every JSON-RPC message (except secrets) to the log buffer.")

(defun zookeeper-bridge-log (fmt &rest args)
  "Append a line built from FMT and ARGS to the bridge log buffer."
  (with-current-buffer (get-buffer-create zookeeper-bridge-log-buffer-name)
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (insert (format-time-string "%H:%M:%S.%3N ")
              (apply #'format fmt args) "\n"))))

(defun zookeeper-bridge--candidates ()
  "Return candidate executable paths for zk-bridge."
  (let* ((exe (if (eq system-type 'windows-nt) "zk-bridge.exe" "zk-bridge"))
         (roots (delete-dups
                 (delq nil (list (locate-dominating-file
                                  zookeeper-bridge--source-dir "Cargo.toml")
                                 (expand-file-name ".." zookeeper-bridge--source-dir))))))
    (append
     (list (expand-file-name (concat "bin/" exe) zookeeper-bridge--source-dir))
     (cl-loop for r in roots
              append (list (expand-file-name (concat "target/release/" exe) r)
                           (expand-file-name (concat "target/debug/" exe) r))))))

(defun zookeeper-bridge-find-executable ()
  "Locate the zk-bridge executable or signal an error."
  (cond
   (zookeeper-bridge-executable
    (or (and (file-name-absolute-p zookeeper-bridge-executable)
             zookeeper-bridge-executable)
        (executable-find zookeeper-bridge-executable)
        zookeeper-bridge-executable))
   ((zookeeper-bridge--remote-directory) "zk-bridge")
   ((executable-find "zk-bridge"))
   ((cl-find-if #'file-executable-p (zookeeper-bridge--candidates)))
   (t (user-error "Cannot find zk-bridge; set `zookeeper-bridge-executable'"))))

(defun zookeeper-bridge--remote-directory ()
  "Return the remote directory to start the bridge in, or nil."
  (pcase zookeeper-bridge-remote
    ('nil nil)
    ('t (and (file-remote-p default-directory) default-directory))
    ((pred stringp) zookeeper-bridge-remote)))

(defun zookeeper-bridge-live-p (&optional bridge)
  "Return non-nil if BRIDGE (default current) has a live process."
  (let ((b (or bridge zookeeper-bridge--current)))
    (and b (process-live-p (zookeeper-bridge-process b)))))

(defun zookeeper-bridge-start ()
  "Start the bridge process and perform the `initialize' handshake.
Return the bridge object."
  (when (zookeeper-bridge-live-p) (zookeeper-bridge-stop))
  (let* ((remote (zookeeper-bridge--remote-directory))
         (default-directory (or remote temporary-file-directory))
         (exe (zookeeper-bridge-find-executable))
         (process-environment (cons (concat "ZK_BRIDGE_LOG=" zookeeper-bridge-log-level)
                                    process-environment))
         (log (get-buffer-create zookeeper-bridge-log-buffer-name))
         (bridge (zookeeper-bridge--make))
         proc)
    (zookeeper-bridge-log "starting %s%s" exe (if remote (concat " on " remote) ""))
    (if remote
        ;; `start-file-process' merges stderr; lines that are not JSON
        ;; are logged by the filter.
        (let ((process-connection-type nil))
          (setq proc (apply #'start-file-process "zk-bridge" nil exe
                            zookeeper-bridge-arguments)))
      (setq proc (make-process
                  :name "zk-bridge"
                  :command (cons exe zookeeper-bridge-arguments)
                  :connection-type 'pipe
                  :coding 'utf-8-unix
                  :noquery t
                  :stderr (make-pipe-process
                           :name "zk-bridge stderr" :buffer log :noquery t
                           :filter #'zookeeper-bridge--stderr-filter))))
    (set-process-coding-system proc 'utf-8-unix 'utf-8-unix)
    (set-process-query-on-exit-flag proc nil)
    (process-put proc 'zookeeper-bridge bridge)
    (set-process-filter proc #'zookeeper-bridge--filter)
    (set-process-sentinel proc #'zookeeper-bridge--sentinel)
    (setf (zookeeper-bridge-process bridge) proc)
    (setq zookeeper-bridge--current bridge)
    (let ((info (zookeeper-rpc "initialize"
                               (list :client zookeeper-bridge-client-name
                                     :protocol_version zookeeper-bridge-protocol-version)
                               10)))
      (zookeeper-bridge-check-version (plist-get info :protocol_version))
      (setf (zookeeper-bridge-info bridge) info)
      (zookeeper-bridge-log "initialized: %s %s protocol %s"
                            (plist-get info :server) (plist-get info :server_version)
                            (plist-get info :protocol_version)))
    (run-hooks 'zookeeper-bridge-restart-hook)
    bridge))

(defun zookeeper-bridge-check-version (server-version)
  "Signal an error unless SERVER-VERSION is compatible with this client."
  (let ((major (lambda (v) (car (split-string (or v "0") "\\.")))))
    (unless (equal (funcall major server-version)
                   (funcall major zookeeper-bridge-protocol-version))
      (zookeeper-signal (list :kind "protocol"
                              :message (format "Incompatible bridge protocol %s (client %s)"
                                               server-version
                                               zookeeper-bridge-protocol-version))))))

(defun zookeeper-bridge-ensure ()
  "Return a live bridge, starting one if necessary."
  (if (zookeeper-bridge-live-p) zookeeper-bridge--current (zookeeper-bridge-start)))

(defun zookeeper-bridge-stop ()
  "Shut the bridge down gracefully."
  (interactive)
  (when-let* ((b zookeeper-bridge--current))
    (setf (zookeeper-bridge-stopping b) t)
    (when (zookeeper-bridge-live-p b)
      (ignore-errors (zookeeper-rpc "shutdown" nil 5))
      (let ((p (zookeeper-bridge-process b)))
        (ignore-errors (process-send-eof p))
        (with-timeout (2 (delete-process p))
          (while (process-live-p p) (accept-process-output p 0.05)))))
    (setq zookeeper-bridge--current nil)))

(defun zookeeper-bridge-restart ()
  "Restart the bridge process.  Clusters must be reconnected."
  (interactive)
  (zookeeper-bridge-stop)
  (zookeeper-bridge-start)
  (message "zk-bridge restarted"))

(defun zookeeper-bridge-show-log ()
  "Display the bridge log buffer."
  (interactive)
  (pop-to-buffer (get-buffer-create zookeeper-bridge-log-buffer-name)))

(defun zookeeper-bridge--stderr-filter (_proc string)
  "Append STRING from bridge stderr to the log buffer."
  (with-current-buffer (get-buffer-create zookeeper-bridge-log-buffer-name)
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (insert string))))

(defun zookeeper-bridge--sentinel (proc event)
  "Handle bridge PROC state change EVENT."
  (unless (process-live-p proc)
    (let ((b (process-get proc 'zookeeper-bridge)))
      (zookeeper-bridge-log "bridge process %s" (string-trim event))
      (when b
        (let ((pending (zookeeper-bridge-pending b)))
          (maphash (lambda (_id entry)
                     (when-let* ((on-error (cdr entry)))
                       (ignore-errors
                         (funcall on-error (list :kind "closed"
                                                 :message "Bridge process exited")))))
                   pending)
          (clrhash pending))
        (when (eq b zookeeper-bridge--current)
          (setq zookeeper-bridge--current nil)
          (run-hook-with-args 'zookeeper-connection-event-functions
                              (list :cluster nil :state "lost"
                                    :event (list :kind "closed"
                                                 :message "Bridge exited")))
          (when (and zookeeper-bridge-auto-restart
                     (not (zookeeper-bridge-stopping b))
                     (not (string-match-p "finished" event)))
            (run-at-time 1 nil
                         (lambda ()
                           (unless (zookeeper-bridge-live-p)
                             (condition-case e
                                 (progn (zookeeper-bridge-start)
                                        (message "zk-bridge restarted after crash; reconnect clusters"))
                               (error (zookeeper-bridge-log "restart failed: %S" e))))))))))))

(defun zookeeper-bridge--filter (proc string)
  "Process filter for bridge PROC receiving STRING."
  (let* ((b (process-get proc 'zookeeper-bridge))
         (split (zookeeper-bridge--split-lines (zookeeper-bridge-partial b) string)))
    (setf (zookeeper-bridge-partial b) (cdr split))
    (dolist (line (car split))
      (zookeeper-bridge--handle-line b line))))

(defun zookeeper-bridge--handle-line (bridge line)
  "Dispatch one LINE of JSON received by BRIDGE."
  (let ((msg (condition-case nil
                 (zookeeper-json-parse line)
               (error nil))))
    (if (not (and msg (listp msg) (plist-member msg :jsonrpc)))
        (zookeeper-bridge-log "%s" line)
      (when zookeeper-bridge-trace (zookeeper-bridge-log "<- %s" line))
      (zookeeper-bridge-dispatch bridge msg))))

(defun zookeeper-bridge-dispatch (bridge msg)
  "Dispatch parsed message MSG for BRIDGE to callbacks or hooks."
  (let ((id (plist-get msg :id))
        (method (plist-get msg :method)))
    (cond
     ((and method (not (plist-member msg :id)))
      (zookeeper-bridge--notify method (plist-get msg :params)))
     ((plist-member msg :id)
      (let ((entry (gethash id (zookeeper-bridge-pending bridge))))
        (remhash id (zookeeper-bridge-pending bridge))
        (when entry
          (if (plist-member msg :error)
              (when (cdr entry)
                (funcall (cdr entry)
                         (zookeeper-bridge--rpc-error-to-plist (plist-get msg :error))))
            (when (car entry)
              (funcall (car entry) (plist-get msg :result))))))))))

(defun zookeeper-bridge--notify (method params)
  "Run hooks for notification METHOD with PARAMS."
  (let ((hook (pcase method
                ("connection.event" 'zookeeper-connection-event-functions)
                ((or "watch.event" "watch.removed") 'zookeeper-watch-event-functions)
                ("cache.event" 'zookeeper-cache-event-functions)
                ("audit.record" 'zookeeper-audit-functions)
                ("recipe.ownership" 'zookeeper-recipe-ownership-functions))))
    (when (and (equal method "watch.removed") (listp params))
      (setq params (plist-put (copy-sequence params) :removed t)))
    (condition-case e
        (progn
          (when hook (run-hook-with-args hook params))
          (run-hook-with-args 'zookeeper-notification-functions method params))
      (error (zookeeper-bridge-log "error in %s handler: %S" method e)))))

;;;; RPC

(defun zookeeper-bridge--send (bridge object)
  "Send OBJECT as one JSON line to BRIDGE."
  (let ((line (zookeeper-json-encode object)))
    (when zookeeper-bridge-trace
      (zookeeper-bridge-log "-> %s" (if (string-match-p "\"value\"" line)
                                        "<request with secret elided>" line)))
    (process-send-string (zookeeper-bridge-process bridge) (concat line "\n"))))

(defun zookeeper-rpc-async (method params &optional on-success on-error)
  "Call METHOD with PARAMS asynchronously.
ON-SUCCESS is called with the result, ON-ERROR with a ZkError plist.
When ON-ERROR is nil, errors are reported with `message'.
Return the request id."
  (let* ((b (zookeeper-bridge-ensure))
         (id (cl-incf (zookeeper-bridge-next-id b))))
    (puthash id (cons on-success
                      (or on-error
                          (lambda (e) (message "zookeeper: %s"
                                               (zookeeper-bridge--error-string e)))))
             (zookeeper-bridge-pending b))
    (zookeeper-bridge--send b (list :jsonrpc "2.0" :id id :method method
                                    :params (or params (make-hash-table))))
    id))

(defun zookeeper-rpc-notify (method params)
  "Send notification METHOD with PARAMS to the bridge."
  (when (zookeeper-bridge-live-p)
    (zookeeper-bridge--send zookeeper-bridge--current
                            (list :jsonrpc "2.0" :method method :params params))))

(defun zookeeper-rpc-cancel (id)
  "Ask the bridge to cancel request ID."
  (zookeeper-rpc-notify "$/cancelRequest" (list :id id)))

(defun zookeeper-rpc (method params &optional timeout)
  "Call METHOD with PARAMS and wait for the result.
Signal `zookeeper-error' on failure.  TIMEOUT defaults to
`zookeeper-bridge-timeout' seconds.  Quitting cancels the request."
  (let* ((done nil) result err
         (b (zookeeper-bridge-ensure))
         (id (zookeeper-rpc-async method params
                                  (lambda (r) (setq result r done t))
                                  (lambda (e) (setq err e done t))))
         (deadline (+ (float-time) (or timeout zookeeper-bridge-timeout)))
         (proc (zookeeper-bridge-process b)))
    (condition-case q
        (while (not done)
          (when (> (float-time) deadline)
            (zookeeper-rpc-cancel id)
            (remhash id (zookeeper-bridge-pending b))
            (zookeeper-signal (list :kind "timeout"
                                    :message (format "%s timed out" method))))
          (unless (process-live-p proc)
            (setq done t err (or err (list :kind "closed" :message "Bridge exited"))))
          (accept-process-output proc 0.05))
      (quit
       (ignore-errors (zookeeper-rpc-cancel id))
       (remhash id (zookeeper-bridge-pending b))
       (signal (car q) (cdr q))))
    (if err (zookeeper-signal err) result)))

(provide 'zookeeper-bridge)
;;; zookeeper-bridge.el ends here
