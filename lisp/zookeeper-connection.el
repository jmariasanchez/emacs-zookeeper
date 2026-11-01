;;; zookeeper-connection.el --- Cluster connections for zookeeper.el  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Cluster profiles, connect/disconnect, connection state tracking, the
;; mode-line lighter, the *zookeeper connections* buffer, faces and
;; small helpers shared by every zookeeper.el module.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'tabulated-list)
(require 'zookeeper-bridge)

(declare-function auth-source-search "auth-source")

(defgroup zookeeper nil
  "ZooKeeper browser and toolkit."
  :group 'tools
  :prefix "zookeeper-")

(defcustom zookeeper-clusters nil
  "Cluster profiles: an alist of (NAME . PLIST).
PLIST keys map to the bridge ClusterConfig:
  :connect STRING            host:port[,host:port][/chroot]   (required)
  :chroot STRING
  :read-only BOOL            refuse every write (enforced by the bridge too)
  :protected-paths LIST      glob patterns requiring typed confirmation/force
  :max-recursive-nodes N
  :session-timeout-ms N, :connection-timeout-ms N, :operation-timeout-ms N
  :allow-read-only BOOL      accept read-only servers
  :auth LIST                 list of (:scheme \"digest\" :credential SECRET)
  :sasl PLIST, :tls PLIST, :retry PLIST   passed through (keys as in protocol)
  :admin-hosts LIST, :server-version STRING
A SECRET is one of (:command (\"pass\" \"show\" \"zk/prod\")), (:env \"VAR\"),
\(:file \"/path\"), (:value \"...\"), or (:auth-source PLIST) where PLIST
is passed to `auth-source-search' (for example (:host \"zk-prod\")).
Auth-source secrets are resolved only at connect time."
  :type '(alist :key-type string :value-type plist))

(defcustom zookeeper-default-read-only nil
  "Read-only default for profiles that do not specify :read-only."
  :type 'boolean)

(defcustom zookeeper-auth-source t
  "When non-nil, allow (:auth-source ...) secrets and fallback lookup.
With a fallback, profiles without :auth look up a digest credential in
auth-source using host \"zookeeper/NAME\".  Found credentials are sent
to the bridge as {\"value\": \"user:password\"} only in `cluster.connect'."
  :type 'boolean)

;;;; Faces

(defface zookeeper-path '((t :inherit font-lock-function-name-face))
  "Face for node paths.")
(defface zookeeper-node '((t :inherit default))
  "Face for plain persistent nodes.")
(defface zookeeper-node-parent '((t :inherit font-lock-keyword-face))
  "Face for nodes with children.")
(defface zookeeper-ephemeral '((t :inherit font-lock-string-face :slant italic))
  "Face for ephemeral nodes.")
(defface zookeeper-sequential '((t :inherit font-lock-constant-face))
  "Face for sequential nodes.")
(defface zookeeper-protected '((t :inherit warning :weight bold))
  "Face for protected paths.")
(defface zookeeper-error '((t :inherit error))
  "Face for errors.")
(defface zookeeper-stale '((t :inherit shadow :slant italic))
  "Face for stale or possibly outdated information.")
(defface zookeeper-marked '((t :inherit dired-marked))
  "Face for marked nodes.")
(defface zookeeper-flagged '((t :inherit dired-flagged))
  "Face for nodes flagged for deletion.")
(defface zookeeper-dim '((t :inherit shadow))
  "Face for secondary information.")
(defface zookeeper-connected '((t :inherit success))
  "Face for connected state.")
(defface zookeeper-suspended '((t :inherit warning))
  "Face for suspended state.")
(defface zookeeper-lost '((t :inherit error))
  "Face for lost or closed state.")
(defface zookeeper-warning-banner '((t :inherit warning :weight bold :height 1.2))
  "Face for big warnings.")

;;;; Profiles and config conversion

(defvar-local zookeeper-cluster nil
  "Cluster profile name associated with the current buffer.")
(put 'zookeeper-cluster 'permanent-local t)

(defun zookeeper-profile (name)
  "Return the profile plist for cluster NAME."
  (or (cdr (assoc name zookeeper-clusters))
      (user-error "Unknown ZooKeeper cluster profile %s" name)))

(defun zookeeper-profile-read-only-p (name)
  "Return non-nil if profile NAME is read-only."
  (let ((p (cdr (assoc name zookeeper-clusters))))
    (if (plist-member p :read-only)
        (zookeeper-true-p (plist-get p :read-only))
      zookeeper-default-read-only)))

(defun zookeeper--bool (v)
  "Convert Lisp boolean V to a JSON boolean."
  (if (zookeeper-true-p v) t :false))

(defun zookeeper--auth-source-lookup (spec)
  "Resolve auth-source SPEC to a \"user:password\" string or nil."
  (require 'auth-source)
  (when-let* ((found (car (apply #'auth-source-search :max 1 spec)))
              (secret (plist-get found :secret)))
    (let ((pw (if (functionp secret) (funcall secret) secret))
          (user (plist-get found :user)))
      (if user (concat user ":" pw) pw))))

(defun zookeeper-secret-to-json (secret &optional resolve)
  "Convert SECRET (see `zookeeper-clusters') to the bridge Secret shape.
When RESOLVE is non-nil, auth-source secrets are looked up; otherwise
they are replaced by a placeholder suitable for display."
  (cond
   ((stringp secret) (list :value secret))
   ((plist-get secret :command)
    (list :command (vconcat (plist-get secret :command))))
   ((plist-get secret :env) (list :env (plist-get secret :env)))
   ((plist-get secret :file) (list :file (expand-file-name (plist-get secret :file))))
   ((plist-member secret :value) (list :value (plist-get secret :value)))
   ((plist-member secret :auth-source)
    (unless zookeeper-auth-source
      (user-error "Auth-source secrets disabled (`zookeeper-auth-source')"))
    (if resolve
        (list :value (or (zookeeper--auth-source-lookup (plist-get secret :auth-source))
                         (user-error "No auth-source entry for %S"
                                     (plist-get secret :auth-source))))
      (list :value "<auth-source>")))
   (t (user-error "Invalid secret specification"))))

(defun zookeeper--keys-to-json (plist)
  "Turn PLIST with dashed keyword keys into one with underscored keys."
  (cl-loop for (k v) on plist by #'cddr
           append (list (intern (replace-regexp-in-string
                                 "-" "_" (symbol-name k)))
                        (cond ((and (listp v) (keywordp (car-safe v)))
                               (zookeeper--keys-to-json v))
                              ((and (listp v) v) (vconcat v))
                              ((eq v nil) :false)
                              (t v)))))

(defun zookeeper-profile-to-config (name &optional resolve)
  "Return the bridge ClusterConfig plist for profile NAME.
Secrets are resolved only when RESOLVE is non-nil.  Never log the result."
  (let* ((p (zookeeper-profile name))
         (cfg (list :name name :connect (or (plist-get p :connect)
                                            (user-error "Profile %s lacks :connect" name))))
         (safety (list :read_only (zookeeper--bool (zookeeper-profile-read-only-p name)))))
    (when-let* ((pp (plist-get p :protected-paths)))
      (setq safety (append safety (list :protected_paths (vconcat pp)))))
    (when-let* ((m (plist-get p :max-recursive-nodes)))
      (setq safety (append safety (list :max_recursive_nodes m))))
    (dolist (k '(:chroot :session-timeout-ms :connection-timeout-ms
                 :operation-timeout-ms :connect-deadline-ms :server-version))
      (when-let* ((v (plist-get p k)))
        (setq cfg (append cfg (zookeeper--keys-to-json (list k v))))))
    (dolist (k '(:allow-read-only :reconnect-on-expiry))
      (when (plist-member p k)
        (setq cfg (append cfg (zookeeper--keys-to-json
                               (list k (zookeeper--bool (plist-get p k))))))))
    (dolist (k '(:tls :retry))
      (when-let* ((v (plist-get p k)))
        (setq cfg (append cfg (zookeeper--keys-to-json (list k v))))))
    (when-let* ((hosts (plist-get p :admin-hosts)))
      (setq cfg (append cfg (list :admin_hosts (vconcat hosts)))))
    (when-let* ((sasl (plist-get p :sasl)))
      (let ((s (zookeeper--keys-to-json (cl-loop for (k v) on sasl by #'cddr
                                                 unless (eq k :password)
                                                 append (list k v)))))
        (when (plist-get sasl :password)
          (setq s (append s (list :password (zookeeper-secret-to-json
                                             (plist-get sasl :password) resolve)))))
        (setq cfg (append cfg (list :sasl s)))))
    (let ((auth (mapcar (lambda (a)
                          (list :scheme (or (plist-get a :scheme) "digest")
                                :credential (zookeeper-secret-to-json
                                             (plist-get a :credential) resolve)))
                        (plist-get p :auth))))
      (when (and (null auth) resolve zookeeper-auth-source
                 (not (plist-member p :auth)))
        (when-let* ((v (ignore-errors
                        (zookeeper--auth-source-lookup
                         (list :host (concat "zookeeper/" name))))))
          (setq auth (list (list :scheme "digest" :credential (list :value v))))))
      (when auth (setq cfg (append cfg (list :auth (vconcat auth))))))
    (append cfg (list :safety safety))))

;;;; Connection state

(defvar zookeeper--connections (make-hash-table :test 'equal)
  "Map of profile name to plist (:id ID :state STATE :event EVENT ...).")

(defvar zookeeper-connection-state-change-hook nil
  "Hook run (with no arguments) when any cluster state changes.")

(defun zookeeper-connection (name)
  "Return the connection plist of cluster NAME, or nil."
  (gethash name zookeeper--connections))

(defun zookeeper--name-for-id (id)
  "Return the profile name connected with bridge cluster ID."
  (catch 'found
    (maphash (lambda (k v) (when (equal (plist-get v :id) id) (throw 'found k)))
             zookeeper--connections)
    nil))

(defun zookeeper-cluster-state (name)
  "Return the state string of cluster NAME, or nil if not connected."
  (plist-get (zookeeper-connection name) :state))

(defun zookeeper-connected-p (name)
  "Return non-nil if cluster NAME has a live bridge handle."
  (and (zookeeper-bridge-live-p)
       (plist-get (zookeeper-connection name) :id)
       (not (member (zookeeper-cluster-state name) '("closed")))))

(defun zookeeper-connect (name)
  "Connect to cluster profile NAME and return the bridge cluster id."
  (interactive (list (zookeeper-read-cluster "Connect to cluster: ")))
  (if (zookeeper-connected-p name)
      (plist-get (zookeeper-connection name) :id)
    (let* ((config (zookeeper-profile-to-config name t))
           (res (let ((message-log-max nil))
                  (zookeeper-rpc "cluster.connect" (list :config config :wait t) 60)))
           (id (plist-get res :cluster)))
      (setq config nil)
      (puthash name (list :id id :state (or (plist-get res :state) "connected")
                          :connected-at (current-time))
               zookeeper--connections)
      (run-hooks 'zookeeper-connection-state-change-hook)
      (force-mode-line-update t)
      (when (called-interactively-p 'interactive)
        (message "Connected to %s (%s)" name id))
      id)))

(defun zookeeper-disconnect (name)
  "Disconnect cluster NAME."
  (interactive (list (completing-read "Disconnect cluster: "
                                      (hash-table-keys zookeeper--connections) nil t)))
  (when-let* ((id (plist-get (zookeeper-connection name) :id)))
    (when (zookeeper-bridge-live-p)
      (condition-case e (zookeeper-rpc "cluster.disconnect" (list :cluster id))
        (zookeeper-error (message "disconnect: %s" (zookeeper-error-message e))))))
  (remhash name zookeeper--connections)
  (run-hooks 'zookeeper-connection-state-change-hook)
  (force-mode-line-update t))

(defun zookeeper-cluster-id (name)
  "Return the bridge id for cluster NAME, connecting if needed."
  (zookeeper-connect name))

(defun zookeeper-call (name method &rest params)
  "Call METHOD on cluster NAME synchronously with plist PARAMS."
  (zookeeper-rpc method (append (list :cluster (zookeeper-cluster-id name)) params)))

(defun zookeeper-call-async (name method params on-success &optional on-error)
  "Call METHOD on cluster NAME with PARAMS asynchronously.
ON-SUCCESS and ON-ERROR as in `zookeeper-rpc-async'."
  (zookeeper-rpc-async method (append (list :cluster (zookeeper-cluster-id name)) params)
                       on-success on-error))

(defun zookeeper--on-connection-event (params)
  "Update state from `connection.event' PARAMS."
  (let* ((id (plist-get params :cluster))
         (event (plist-get params :event))
         (state (or (plist-get params :state) (plist-get event :kind))))
    (if (null id)
        ;; Bridge exited: every connection is gone.
        (clrhash zookeeper--connections)
      (when-let* ((name (zookeeper--name-for-id id)))
        (let ((c (zookeeper-connection name)))
          (setq c (plist-put c :state state))
          (setq c (plist-put c :event event))
          (when (plist-get event :session_id)
            (setq c (plist-put c :session (plist-get event :session_id))))
          (puthash name c zookeeper--connections))))
    (run-hooks 'zookeeper-connection-state-change-hook)
    (force-mode-line-update t)))

(add-hook 'zookeeper-connection-event-functions #'zookeeper--on-connection-event)

(defun zookeeper-read-cluster (&optional prompt)
  "Read a cluster profile name with PROMPT."
  (unless zookeeper-clusters
    (user-error "No clusters configured; customize `zookeeper-clusters'"))
  (if (= (length zookeeper-clusters) 1)
      (caar zookeeper-clusters)
    (completing-read (or prompt "Cluster: ") (mapcar #'car zookeeper-clusters)
                     nil t nil nil (or zookeeper-cluster (caar zookeeper-clusters)))))

(defun zookeeper-current-cluster ()
  "Return the buffer's cluster, or read one."
  (or zookeeper-cluster (zookeeper-read-cluster)))

;;;; Mode line

(defun zookeeper-state-face (state)
  "Return the face for connection STATE."
  (pcase state
    ((or "connected" "reconnected" "session-reestablished") 'zookeeper-connected)
    ((or "suspended" "read-only") 'zookeeper-suspended)
    (_ 'zookeeper-lost)))

(defun zookeeper-mode-line ()
  "Return the mode-line lighter for the buffer's cluster."
  (when zookeeper-cluster
    (let ((state (or (zookeeper-cluster-state zookeeper-cluster) "disconnected")))
      (concat " " (propertize (format "zk:%s[%s]%s" zookeeper-cluster state
                                      (if (zookeeper-profile-read-only-p zookeeper-cluster)
                                          ":RO" ""))
                              'face (zookeeper-state-face state))))))

(defvar zookeeper-mode-line-format '(:eval (zookeeper-mode-line))
  "Mode-line construct showing cluster state.")
(put 'zookeeper-mode-line-format 'risky-local-variable t)

(defun zookeeper-setup-mode-line ()
  "Install the cluster lighter in the current buffer."
  (setq-local mode-line-process zookeeper-mode-line-format))

;;;; Shared helpers

(defun zookeeper-path-join (parent child)
  "Join PARENT path and CHILD name."
  (if (equal parent "/") (concat "/" child) (concat parent "/" child)))

(defun zookeeper-path-parent (path)
  "Return the parent of PATH (\"/\" for top-level nodes)."
  (let ((p (file-name-directory (directory-file-name path))))
    (if (or (null p) (equal p "/")) "/" (directory-file-name p))))

(defun zookeeper-path-name (path)
  "Return the last component of PATH."
  (if (equal path "/") "/" (file-name-nondirectory path)))

(defun zookeeper-format-time (ms)
  "Format epoch milliseconds MS for humans."
  (if (and (numberp ms) (> ms 0))
      (format-time-string "%Y-%m-%d %H:%M:%S" (/ ms 1000.0))
    "-"))

(defun zookeeper-format-session (owner)
  "Format ephemeral OWNER session id as hex, or nil if zero."
  (when (and (numberp owner) (/= owner 0))
    (format "0x%x" owner)))

(defun zookeeper-format-size (n)
  "Format byte count N."
  (if (numberp n) (file-size-human-readable n) "-"))

(defun zookeeper-check-writable (name)
  "Signal a `user-error' if cluster NAME is read-only."
  (when (zookeeper-profile-read-only-p name)
    (user-error "Cluster %s is read-only" name)))

(defun zookeeper-confirm-typed (prompt &optional word)
  "Require the user to type WORD (default \"yes\") after PROMPT."
  (let ((w (or word "yes")))
    (unless (equal (read-string (format "%s Type `%s' to confirm: " prompt w)) w)
      (user-error "Not confirmed"))
    t))

(defun zookeeper-confirm-force (what)
  "Ask twice before using force for WHAT.  Return non-nil when accepted."
  (and (yes-or-no-p (format "%s is protected.  Bypass protection with force? " what))
       (zookeeper-confirm-typed (format "Really force %s?" what) "force")))

(defun zookeeper-error-string (err)
  "Return a readable string for condition or ZkError plist ERR."
  (zookeeper-bridge--error-string (zookeeper-error-data err)))

;;;; Connections buffer

(defvar zookeeper-connections-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m (kbd "RET") #'zookeeper-connections-browse)
    (define-key m "c" #'zookeeper-connections-connect)
    (define-key m "d" #'zookeeper-connections-disconnect)
    (define-key m "i" #'zookeeper-connections-diagnostics)
    (define-key m "l" #'zookeeper-bridge-show-log)
    m)
  "Keymap for `zookeeper-connections-mode'.")

(define-derived-mode zookeeper-connections-mode tabulated-list-mode "ZK-Connections"
  "List of configured ZooKeeper clusters and their state.
\\{zookeeper-connections-mode-map}"
  (setq tabulated-list-format [("Cluster" 16 t) ("State" 22 t) ("Id" 5 t)
                               ("RO" 3 t) ("Session" 20 nil) ("Connect" 40 nil)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'zookeeper-connections--refresh nil t)
  (tabulated-list-init-header))

(defun zookeeper-connections--refresh ()
  "Fill `tabulated-list-entries' for the connections buffer."
  (setq tabulated-list-entries
        (mapcar (lambda (entry)
                  (let* ((name (car entry))
                         (c (zookeeper-connection name))
                         (state (or (plist-get c :state) "disconnected")))
                    (list name
                          (vector name
                                  (propertize state 'face (zookeeper-state-face state))
                                  (or (plist-get c :id) "")
                                  (if (zookeeper-profile-read-only-p name) "yes" "")
                                  (format "%s" (or (plist-get c :session) ""))
                                  (or (plist-get (cdr entry) :connect) "")))))
                zookeeper-clusters)))

(defun zookeeper-connections--update ()
  "Refresh the connections buffer if visible."
  (when-let* ((b (get-buffer "*zookeeper connections*")))
    (with-current-buffer b
      (zookeeper-connections--refresh)
      (tabulated-list-print t))))

(add-hook 'zookeeper-connection-state-change-hook #'zookeeper-connections--update)

;;;###autoload
(defun zookeeper-list-connections ()
  "Show configured clusters and their connection state."
  (interactive)
  (with-current-buffer (get-buffer-create "*zookeeper connections*")
    (zookeeper-connections-mode)
    (zookeeper-connections--refresh)
    (tabulated-list-print)
    (pop-to-buffer (current-buffer))))

(defun zookeeper-connections-connect ()
  "Connect the cluster at point."
  (interactive)
  (when-let* ((n (tabulated-list-get-id)))
    (zookeeper-connect n)
    (zookeeper-connections--update)))

(defun zookeeper-connections-disconnect ()
  "Disconnect the cluster at point."
  (interactive)
  (when-let* ((n (tabulated-list-get-id))) (zookeeper-disconnect n)))

(declare-function zookeeper-browse "zookeeper-browser")
(declare-function zookeeper-cluster-status "zookeeper-diagnostics")

(defun zookeeper-connections-browse ()
  "Browse the cluster at point."
  (interactive)
  (when-let* ((n (tabulated-list-get-id))) (zookeeper-browse n)))

(defun zookeeper-connections-diagnostics ()
  "Show diagnostics for the cluster at point."
  (interactive)
  (when-let* ((n (tabulated-list-get-id))) (zookeeper-cluster-status n)))

(provide 'zookeeper-connection)
;;; zookeeper-connection.el ends here
