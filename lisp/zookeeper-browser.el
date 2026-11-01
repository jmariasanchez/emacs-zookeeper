;;; zookeeper-browser.el --- Tree browser for zookeeper.el  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; A dired-like, lazily expanded tree of ZooKeeper nodes.  One buffer
;; per cluster.  Expanded nodes are kept fresh with persistent watches.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'bookmark)
(require 'zookeeper-connection)

(declare-function zookeeper-node-open "zookeeper-node")
(declare-function zookeeper-acl-open "zookeeper-acl")
(declare-function zookeeper-recipe-inspect "zookeeper-recipe")
(declare-function zookeeper-watch-add "zookeeper-watch")

(defgroup zookeeper-browser nil
  "ZooKeeper tree browser."
  :group 'zookeeper
  :prefix "zookeeper-browser-")

(defcustom zookeeper-browser-page-size 500
  "Number of children fetched per page when expanding a node."
  :type 'integer)

(defcustom zookeeper-browser-live-refresh t
  "When non-nil, expanded nodes are refreshed by persistent watches."
  :type 'boolean)

(defcustom zookeeper-browser-large-delete-threshold 50
  "Recursive deletes of more nodes than this require typed confirmation."
  :type 'integer)

(defcustom zookeeper-browser-icons
  '((parent-open . "▾") (parent-closed . "▸") (leaf . "·")
    (ephemeral . "⚡") (sequential . "#"))
  "Icons used in the tree."
  :type '(alist :key-type symbol :value-type string))

(defcustom zookeeper-browser-mode-hook nil
  "Hook run when entering `zookeeper-browser-mode'."
  :type 'hook)

(defvar zookeeper-recent-paths nil
  "Recently visited (CLUSTER . PATH) pairs, most recent first.")

(defcustom zookeeper-recent-paths-max 50
  "Maximum length of `zookeeper-recent-paths'."
  :type 'integer)

;;;; Buffer state

(defvar-local zookeeper-browser--root "/" "Root path shown in this browser.")
(defvar-local zookeeper-browser--expanded nil "Hash: path -> t when expanded.")
(defvar-local zookeeper-browser--children nil
  "Hash: path -> plist (:children LIST :total N :stat STAT :error ERR).")
(defvar-local zookeeper-browser--marks nil "Hash: path -> mark char.")
(defvar-local zookeeper-browser--watches nil "Hash: path -> watch id.")

(defun zookeeper-browser--init-state ()
  "Initialize the state tables of the current buffer."
  (setq zookeeper-browser--expanded (make-hash-table :test 'equal)
        zookeeper-browser--children (make-hash-table :test 'equal)
        zookeeper-browser--marks (make-hash-table :test 'equal)
        zookeeper-browser--watches (make-hash-table :test 'equal)))

;;;; Rendering (pure-ish: depends only on buffer state)

(defun zookeeper-browser--sequential-p (name)
  "Return non-nil if NAME looks like a sequential node name."
  (string-match-p "[0-9]\\{10\\}\\'" name))

(defun zookeeper-browser--protected-p (path)
  "Return non-nil if PATH matches a protected pattern of this cluster."
  (when zookeeper-cluster
    (cl-some (lambda (glob) (zookeeper-glob-match-p glob path))
             (plist-get (cdr (assoc zookeeper-cluster zookeeper-clusters))
                        :protected-paths))))

(defun zookeeper-glob-match-p (glob path)
  "Return non-nil if PATH matches GLOB (`*' one segment, `**' any depth)."
  (let ((re "") (i 0) (n (length glob)))
    (while (< i n)
      (cond
       ((and (< (1+ i) n) (eq (aref glob i) ?*) (eq (aref glob (1+ i)) ?*))
        (setq re (concat re ".*") i (+ i 2)))
       ((eq (aref glob i) ?*) (setq re (concat re "[^/]*") i (1+ i)))
       ((eq (aref glob i) ??) (setq re (concat re "[^/]") i (1+ i)))
       (t (setq re (concat re (regexp-quote (string (aref glob i)))) i (1+ i)))))
    ;; "/a/**" also matches "/a" itself.
    (when (string-suffix-p "/.*" re)
      (setq re (concat (substring re 0 -3) "\\(?:/.*\\)?")))
    (string-match-p (concat "\\`" re "\\'") path)))

(defun zookeeper-browser--node-face (child)
  "Return the face for CHILD entry."
  (let* ((stat (plist-get child :stat))
         (path (plist-get child :path)))
    (cond
     ((zookeeper-browser--protected-p path) 'zookeeper-protected)
     ((and stat (numberp (plist-get stat :ephemeral_owner))
           (/= 0 (plist-get stat :ephemeral_owner)))
      'zookeeper-ephemeral)
     ((zookeeper-browser--sequential-p (plist-get child :name)) 'zookeeper-sequential)
     ((and stat (> (or (plist-get stat :num_children) 0) 0)) 'zookeeper-node-parent)
     (t 'zookeeper-node))))

(defun zookeeper-browser--icon (key)
  "Return icon string for KEY."
  (or (alist-get key zookeeper-browser-icons) " "))

(defun zookeeper-browser--format-line (child depth)
  "Return the display line for CHILD at DEPTH."
  (let* ((path (plist-get child :path))
         (name (plist-get child :name))
         (stat (plist-get child :stat))
         (nkids (and stat (plist-get stat :num_children)))
         (ephemeral (and stat (numberp (plist-get stat :ephemeral_owner))
                         (/= 0 (plist-get stat :ephemeral_owner))))
         (expanded (gethash path zookeeper-browser--expanded))
         (mark (gethash path zookeeper-browser--marks))
         (icon (cond ((and nkids (> nkids 0))
                      (zookeeper-browser--icon (if expanded 'parent-open 'parent-closed)))
                     ((and (null stat) (not expanded)) (zookeeper-browser--icon 'parent-closed))
                     (expanded (zookeeper-browser--icon 'parent-open))
                     (t (zookeeper-browser--icon 'leaf))))
         (line (concat
                (cond ((eq mark ?D) (propertize "D" 'face 'zookeeper-flagged))
                      (mark (propertize (char-to-string mark) 'face 'zookeeper-marked))
                      (t " "))
                " "
                (make-string (* 2 depth) ?\s)
                icon " "
                (propertize name 'face (zookeeper-browser--node-face child))
                (if ephemeral (concat " " (zookeeper-browser--icon 'ephemeral)) "")
                (if (and nkids (> nkids 0))
                    (propertize (format "  (%d)" nkids) 'face 'zookeeper-dim) "")
                (if (and stat (> (or (plist-get stat :data_length) 0) 0))
                    (propertize (format "  %s" (zookeeper-format-size
                                                (plist-get stat :data_length)))
                                'face 'zookeeper-dim)
                  ""))))
    (propertize line 'zookeeper-path path 'zookeeper-node child)))

(defun zookeeper-browser--insert-children (path depth)
  "Insert the cached children of PATH at DEPTH, recursively for expanded ones."
  (let ((entry (gethash path zookeeper-browser--children)))
    (cond
     ((plist-get entry :error)
      (insert (make-string (+ 2 (* 2 depth)) ?\s)
              (propertize (format "error: %s" (zookeeper-error-string (plist-get entry :error)))
                          'face 'zookeeper-error)
              "\n"))
     ((null entry)
      (insert (make-string (+ 2 (* 2 depth)) ?\s)
              (propertize "loading..." 'face 'zookeeper-stale) "\n"))
     (t
      (dolist (child (plist-get entry :children))
        (insert (zookeeper-browser--format-line child depth) "\n")
        (when (gethash (plist-get child :path) zookeeper-browser--expanded)
          (zookeeper-browser--insert-children (plist-get child :path) (1+ depth))))
      (let ((shown (length (plist-get entry :children)))
            (total (or (plist-get entry :total) 0)))
        (when (< shown total)
          (insert (propertize
                   (concat "  " (make-string (* 2 depth) ?\s)
                           (format "more... (%d of %d shown, RET to load %d more)"
                                   shown total (min zookeeper-browser-page-size
                                                    (- total shown))))
                   'face 'zookeeper-dim 'zookeeper-more path)
                  "\n")))))))

(defun zookeeper-browser-render ()
  "Redraw the current browser buffer, keeping point on the same path."
  (let ((inhibit-read-only t)
        (path (zookeeper-browser-path-at-point))
        (line (line-number-at-pos))
        (root zookeeper-browser--root))
    (erase-buffer)
    (insert (propertize (format "%s:%s" zookeeper-cluster root) 'face 'zookeeper-path)
            (if (zookeeper-profile-read-only-p zookeeper-cluster)
                (propertize "  [read-only]" 'face 'zookeeper-dim) "")
            "\n")
    (insert (propertize (zookeeper-browser--format-line
                         (list :name root :path root
                               :stat (plist-get (gethash root zookeeper-browser--children) :stat))
                         0)
                        'zookeeper-path root)
            "\n")
    (when (gethash root zookeeper-browser--expanded)
      (zookeeper-browser--insert-children root 1))
    (unless (and path (zookeeper-browser--goto-line-of path))
      (goto-char (point-min))
      (forward-line (1- line)))))

(defun zookeeper-browser--goto-line-of (path)
  "Move point to the line showing PATH.  Return non-nil on success."
  (let ((pos (point-min)) found)
    (while (and (not found) pos (< pos (point-max)))
      (if (equal (get-text-property pos 'zookeeper-path) path)
          (setq found pos)
        (setq pos (next-single-property-change pos 'zookeeper-path))))
    (when found (goto-char found) (beginning-of-line) t)))

(defun zookeeper-browser-path-at-point ()
  "Return the node path on the current line, or nil."
  (get-text-property (line-beginning-position) 'zookeeper-path))

;;;; Fetching

(defun zookeeper-browser--fetch (path &optional append callback)
  "Fetch children of PATH into the cache.
When APPEND is non-nil fetch the next page.  CALLBACK is run after."
  (let* ((buf (current-buffer))
         (old (gethash path zookeeper-browser--children))
         (offset (if append (length (plist-get old :children)) 0)))
    (zookeeper-call-async
     zookeeper-cluster "node.children"
     (list :path path :stats t :limit zookeeper-browser-page-size :offset offset)
     (lambda (res)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (puthash path
                    (list :children (append (and append (plist-get old :children))
                                            (plist-get res :children))
                          :total (plist-get res :total)
                          :stat (plist-get res :stat))
                    zookeeper-browser--children)
           (zookeeper-browser-render)
           (when callback (funcall callback)))))
     (lambda (err)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (if (equal (plist-get err :kind) "no-node")
               (progn (remhash path zookeeper-browser--expanded)
                      (remhash path zookeeper-browser--children))
             (puthash path (list :error err) zookeeper-browser--children))
           (zookeeper-browser-render)))))))

(defun zookeeper-browser--fetch-sync (path)
  "Fetch the first page of children of PATH synchronously."
  (let ((res (zookeeper-call zookeeper-cluster "node.children"
                             :path path :stats t :limit zookeeper-browser-page-size)))
    (puthash path (list :children (plist-get res :children)
                        :total (plist-get res :total)
                        :stat (plist-get res :stat))
             zookeeper-browser--children)))

;;;; Live refresh: children caches, falling back to persistent watches

(defvar zookeeper-browser--cache-unsupported nil
  "Non-nil when the bridge does not implement cache.* methods.")

(defvar-local zookeeper-browser--caches nil
  "Hash: path -> cache id (or `pending').")
(defvar-local zookeeper-browser--cache-states nil
  "Hash: cache id -> freshness state string.")

(defconst zookeeper-browser--state-rank
  '(("live" . 0) ("initializing" . 1) ("resyncing" . 2) ("suspended" . 3)
    ("stale" . 4) ("closed" . 5))
  "Severity of cache states, higher is worse.")

(defun zookeeper-browser-freshness ()
  "Return the worst cache state of this browser, or nil when no caches."
  (let (worst)
    (when zookeeper-browser--cache-states
      (maphash (lambda (_ s)
                 (when (> (alist-get s zookeeper-browser--state-rank 5 nil #'equal)
                          (alist-get worst zookeeper-browser--state-rank -1 nil #'equal))
                   (setq worst s)))
               zookeeper-browser--cache-states))
    worst))

(defun zookeeper-browser--header-line ()
  "Header line showing live-refresh freshness."
  (let ((s (zookeeper-browser-freshness)))
    (cond
     (s (concat " cache: "
                (propertize s 'face (pcase s
                                      ("live" 'zookeeper-connected)
                                      ((or "initializing" "resyncing" "suspended")
                                       'zookeeper-suspended)
                                      (_ 'zookeeper-stale)))
                (if (equal s "live") ""
                  (propertize "  [STALE: tree may be outdated]" 'face 'zookeeper-stale))))
     ((and zookeeper-browser--watches (> (hash-table-count zookeeper-browser--watches) 0))
      " live refresh: watches")
     (t nil))))

(defun zookeeper-browser--set-cache-state (cache state)
  "Record STATE for CACHE and update the header line."
  (puthash cache state zookeeper-browser--cache-states)
  (force-mode-line-update))

(defun zookeeper-browser--watch (path)
  "Keep PATH fresh with a children cache, or a persistent watch as fallback."
  (unless zookeeper-browser--caches
    (setq zookeeper-browser--caches (make-hash-table :test 'equal)
          zookeeper-browser--cache-states (make-hash-table :test 'equal)))
  (cond
   ((not zookeeper-browser-live-refresh) nil)
   ((gethash path zookeeper-browser--caches) nil)
   (zookeeper-browser--cache-unsupported (zookeeper-browser--watch-fallback path))
   (t
    (let ((buf (current-buffer)))
      (puthash path 'pending zookeeper-browser--caches)
      (zookeeper-call-async
       zookeeper-cluster "cache.start" (list :path path :kind "children")
       (lambda (res)
         (when (buffer-live-p buf)
           (with-current-buffer buf
             (let ((id (plist-get res :cache)))
               (if (gethash path zookeeper-browser--expanded)
                   (progn
                     (puthash path id zookeeper-browser--caches)
                     (zookeeper-browser--set-cache-state
                      id (or (plist-get (plist-get res :status) :state) "live")))
                 (remhash path zookeeper-browser--caches)
                 (zookeeper-rpc-async "cache.stop" (list :cache id) #'ignore #'ignore))))))
       (lambda (err)
         (when (buffer-live-p buf)
           (with-current-buffer buf
             (remhash path zookeeper-browser--caches)
             (when (equal (plist-get err :kind) "method-not-found")
               (setq zookeeper-browser--cache-unsupported t))
             (zookeeper-browser--watch-fallback path)))))))))

(defun zookeeper-browser--uncache (path)
  "Stop the cache or watch keeping PATH fresh."
  (when zookeeper-browser--caches
    (let ((c (gethash path zookeeper-browser--caches)))
      (remhash path zookeeper-browser--caches)
      (when (stringp c)
        (remhash c zookeeper-browser--cache-states)
        (when (zookeeper-bridge-live-p)
          (zookeeper-rpc-async "cache.stop" (list :cache c) #'ignore #'ignore)))))
  (zookeeper-browser--unwatch path))

(defun zookeeper-browser--on-cache-event (params)
  "Refresh browsers affected by cache event PARAMS."
  (let* ((cache (plist-get params :cache))
         (event (plist-get params :event))
         (type (plist-get event :type)))
    (dolist (buf (buffer-list))
      (with-current-buffer buf
        (when (and (derived-mode-p 'zookeeper-browser-mode) zookeeper-browser--caches)
          (let (hit)
            (maphash (lambda (p c) (when (equal c cache) (setq hit p)))
                     zookeeper-browser--caches)
            (when hit
              (pcase type
                ((or "initialized" "resynced")
                 (zookeeper-browser--set-cache-state cache "live")
                 (zookeeper-browser--fetch hit))
                ("resyncing" (zookeeper-browser--set-cache-state cache "resyncing"))
                ("stale" (zookeeper-browser--set-cache-state cache "stale"))
                ("error" (zookeeper-browser--set-cache-state cache "stale"))
                ("closed" (zookeeper-browser--set-cache-state cache "closed"))
                ((or "node-added" "node-removed" "node-updated" "overflow")
                 (if (and (equal type "node-removed")
                          (equal (plist-get event :path) hit))
                     (let ((parent (zookeeper-path-parent hit)))
                       (when (gethash parent zookeeper-browser--expanded)
                         (zookeeper-browser--fetch parent)))
                   (when (gethash hit zookeeper-browser--expanded)
                     (zookeeper-browser--fetch hit))))))))))))

(add-hook 'zookeeper-cache-event-functions #'zookeeper-browser--on-cache-event)

(defun zookeeper-browser--watch-fallback (path)
  "Install a live-refresh persistent watch on PATH."
  (when (and zookeeper-browser-live-refresh
             (not (gethash path zookeeper-browser--watches)))
    (let ((buf (current-buffer)))
      (puthash path 'pending zookeeper-browser--watches)
      (zookeeper-call-async
       zookeeper-cluster "watch.add" (list :path path :kind "persistent")
       (lambda (res)
         (when (buffer-live-p buf)
           (with-current-buffer buf
             (if (gethash path zookeeper-browser--expanded)
                 (puthash path (plist-get res :watch) zookeeper-browser--watches)
               (remhash path zookeeper-browser--watches)
               (zookeeper-rpc-async "watch.remove" (list :watch (plist-get res :watch))
                                    #'ignore #'ignore)))))
       (lambda (_err)
         (when (buffer-live-p buf)
           (with-current-buffer buf (remhash path zookeeper-browser--watches))))))))

(defun zookeeper-browser--unwatch (path)
  "Remove the live-refresh watch on PATH."
  (let ((w (gethash path zookeeper-browser--watches)))
    (remhash path zookeeper-browser--watches)
    (when (and (stringp w) (zookeeper-bridge-live-p))
      (zookeeper-rpc-async "watch.remove" (list :watch w) #'ignore #'ignore))))

(defun zookeeper-browser--on-watch-event (params)
  "Refresh browsers affected by watch event PARAMS."
  (let ((watch (plist-get params :watch))
        (kind (plist-get (plist-get params :event) :kind)))
    (dolist (buf (buffer-list))
      (with-current-buffer buf
        (when (and (derived-mode-p 'zookeeper-browser-mode) zookeeper-browser--watches)
          (let (hit)
            (maphash (lambda (p w) (when (equal w watch) (setq hit p)))
                     zookeeper-browser--watches)
            (when hit
              (cond
               ((plist-get params :removed) (remhash hit zookeeper-browser--watches))
               ((equal kind "node-deleted")
                (let ((parent (zookeeper-path-parent hit)))
                  (when (gethash parent zookeeper-browser--expanded)
                    (zookeeper-browser--fetch parent))))
               ((member kind '("node-children-changed" "node-created" "resync"
                               "node-data-changed" "reregistered"))
                (when (gethash hit zookeeper-browser--expanded)
                  (zookeeper-browser--fetch hit)))))))))))

(add-hook 'zookeeper-watch-event-functions #'zookeeper-browser--on-watch-event)

;;;; Mode

(defvar zookeeper-browser-mode-map
  (let ((m (make-sparse-keymap)))
    (set-keymap-parent m special-mode-map)
    (define-key m (kbd "TAB") #'zookeeper-browser-toggle)
    (define-key m (kbd "<backtab>") #'zookeeper-browser-collapse-all)
    (define-key m (kbd "RET") #'zookeeper-browser-visit)
    (define-key m "o" #'zookeeper-browser-toggle)
    (define-key m "v" #'zookeeper-browser-visit)
    (define-key m "^" #'zookeeper-browser-up)
    (define-key m "n" #'next-line)
    (define-key m "p" #'previous-line)
    (define-key m "m" #'zookeeper-browser-mark)
    (define-key m "u" #'zookeeper-browser-unmark)
    (define-key m "U" #'zookeeper-browser-unmark-all)
    (define-key m "d" #'zookeeper-browser-flag-deletion)
    (define-key m "x" #'zookeeper-browser-execute)
    (define-key m "*" #'zookeeper-browser-mark-regexp)
    (define-key m "c" #'zookeeper-browser-create)
    (define-key m "e" #'zookeeper-browser-edit)
    (define-key m "D" #'zookeeper-browser-delete)
    (define-key m "C" #'zookeeper-browser-copy)
    (define-key m "R" #'zookeeper-browser-rename)
    (define-key m "g" #'zookeeper-browser-refresh)
    (define-key m "s" #'zookeeper-search)
    (define-key m "=" #'zookeeper-browser-diff)
    (define-key m "a" #'zookeeper-browser-acl)
    (define-key m "r" #'zookeeper-browser-recipe)
    (define-key m "W" #'zookeeper-browser-watch)
    (define-key m "w" #'zookeeper-browser-copy-path)
    (define-key m "j" #'zookeeper-goto-path)
    (define-key m "?" #'zookeeper-dispatch)
    m)
  "Keymap for `zookeeper-browser-mode'.")

(declare-function zookeeper-dispatch "zookeeper")

(define-derived-mode zookeeper-browser-mode special-mode "ZK-Browser"
  "Browse a ZooKeeper tree.
\\{zookeeper-browser-mode-map}"
  (setq truncate-lines t)
  (setq-local revert-buffer-function (lambda (&rest _) (zookeeper-browser-refresh)))
  (setq-local bookmark-make-record-function #'zookeeper-browser-bookmark-make-record)
  (add-hook 'kill-buffer-hook #'zookeeper-browser--cleanup nil t)
  (zookeeper-setup-mode-line)
  (setq header-line-format '(:eval (zookeeper-browser--header-line)))
  (unless zookeeper-browser--expanded (zookeeper-browser--init-state)))

(defun zookeeper-browser--cleanup ()
  "Remove caches and watches owned by this browser buffer."
  (when zookeeper-browser--caches
    (dolist (p (hash-table-keys zookeeper-browser--caches))
      (ignore-errors (zookeeper-browser--uncache p))))
  (when zookeeper-browser--watches
    (dolist (p (hash-table-keys zookeeper-browser--watches))
      (ignore-errors (zookeeper-browser--unwatch p)))))

(defun zookeeper-browser-buffer-name (cluster)
  "Return the browser buffer name for CLUSTER."
  (format "*zookeeper: %s*" cluster))

;;;###autoload
(defun zookeeper-browse (cluster &optional path)
  "Open the tree browser for CLUSTER, optionally showing PATH."
  (interactive (list (zookeeper-read-cluster "Browse cluster: ")))
  (zookeeper-connect cluster)
  (let ((buf (get-buffer-create (zookeeper-browser-buffer-name cluster))))
    (with-current-buffer buf
      (unless (derived-mode-p 'zookeeper-browser-mode)
        (zookeeper-browser-mode)
        (setq zookeeper-cluster cluster)
        (zookeeper-browser--expand "/" t)))
    (pop-to-buffer-same-window buf)
    (when path (zookeeper-browser-reveal path))
    buf))

(defun zookeeper-browser--expand (path &optional sync)
  "Expand PATH, fetching its children (synchronously when SYNC)."
  (puthash path t zookeeper-browser--expanded)
  (if sync
      (condition-case err (zookeeper-browser--fetch-sync path)
        (zookeeper-error (puthash path (list :error (zookeeper-error-data err))
                                  zookeeper-browser--children)))
    (zookeeper-browser--fetch path))
  (zookeeper-browser--watch path)
  (zookeeper-browser-render))

(defun zookeeper-browser--collapse (path)
  "Collapse PATH."
  (remhash path zookeeper-browser--expanded)
  (zookeeper-browser--uncache path)
  (zookeeper-browser-render))

(defun zookeeper-browser-toggle ()
  "Expand or collapse the node at point, or load more children."
  (interactive)
  (if-let* ((more (get-text-property (line-beginning-position) 'zookeeper-more)))
      (zookeeper-browser--fetch more t)
    (let ((path (or (zookeeper-browser-path-at-point) (user-error "No node here"))))
      (if (gethash path zookeeper-browser--expanded)
          (zookeeper-browser--collapse path)
        (zookeeper-browser--expand path)))))

(defun zookeeper-browser-collapse-all ()
  "Collapse everything below the root."
  (interactive)
  (dolist (p (hash-table-keys zookeeper-browser--expanded))
    (unless (equal p zookeeper-browser--root) (zookeeper-browser--collapse p))))

(defun zookeeper-browser-visit ()
  "Open the node at point in the inspector, or toggle if it has children."
  (interactive)
  (if (get-text-property (line-beginning-position) 'zookeeper-more)
      (zookeeper-browser-toggle)
    (let* ((path (or (zookeeper-browser-path-at-point) (user-error "No node here")))
           (stat (plist-get (get-text-property (line-beginning-position) 'zookeeper-node)
                            :stat)))
      (if (and stat (> (or (plist-get stat :num_children) 0) 0)
               (= 0 (or (plist-get stat :data_length) 0))
               (not current-prefix-arg))
          (zookeeper-browser-toggle)
        (zookeeper-browser-open-node path)))))

(defun zookeeper-browser-open-node (path)
  "Open PATH in the node inspector."
  (require 'zookeeper-node)
  (zookeeper-add-recent zookeeper-cluster path)
  (zookeeper-node-open zookeeper-cluster path))

(defun zookeeper-add-recent (cluster path)
  "Record CLUSTER PATH as recently visited."
  (let ((e (cons cluster path)))
    (setq zookeeper-recent-paths
          (seq-take (cons e (delete e zookeeper-recent-paths)) zookeeper-recent-paths-max))))

(defun zookeeper-browser-up ()
  "Move to the parent node line."
  (interactive)
  (when-let* ((p (zookeeper-browser-path-at-point)))
    (zookeeper-browser--goto-line-of (zookeeper-path-parent p))))

(defun zookeeper-browser-reveal (path)
  "Expand ancestors of PATH and move point to it."
  (let ((parts (split-string path "/" t)) (cur "/"))
    (dolist (part (butlast parts))
      (setq cur (zookeeper-path-join cur part))
      (unless (gethash cur zookeeper-browser--expanded)
        (zookeeper-browser--expand cur t)))
    (let ((parent (zookeeper-path-parent path)))
      (unless (gethash parent zookeeper-browser--expanded)
        (zookeeper-browser--expand parent t)))
    (zookeeper-browser-render)
    (zookeeper-browser--goto-line-of path)))

(defun zookeeper-browser-refresh ()
  "Refetch every expanded node."
  (interactive)
  (dolist (p (hash-table-keys zookeeper-browser--expanded))
    (condition-case err (zookeeper-browser--fetch-sync p)
      (zookeeper-error
       (if (equal (zookeeper-error-kind err) "no-node")
           (progn (remhash p zookeeper-browser--expanded)
                  (remhash p zookeeper-browser--children))
         (puthash p (list :error (zookeeper-error-data err)) zookeeper-browser--children)))))
  (zookeeper-browser-render)
  (message "Refreshed"))

(defun zookeeper-browser-copy-path ()
  "Copy the path at point to the kill ring."
  (interactive)
  (let ((p (or (zookeeper-browser-path-at-point) (user-error "No node here"))))
    (kill-new p)
    (message "%s" p)))

;;;; Marks

(defun zookeeper-browser--set-mark (char)
  "Set mark CHAR (nil to clear) on the node at point and move down."
  (when-let* ((p (zookeeper-browser-path-at-point)))
    (if char (puthash p char zookeeper-browser--marks)
      (remhash p zookeeper-browser--marks))
    (zookeeper-browser-render)
    (forward-line 1)))

(defun zookeeper-browser-mark ()
  "Mark the node at point."
  (interactive)
  (zookeeper-browser--set-mark ?*))

(defun zookeeper-browser-unmark ()
  "Unmark the node at point."
  (interactive)
  (zookeeper-browser--set-mark nil))

(defun zookeeper-browser-flag-deletion ()
  "Flag the node at point for deletion."
  (interactive)
  (zookeeper-browser--set-mark ?D))

(defun zookeeper-browser-unmark-all ()
  "Remove all marks."
  (interactive)
  (clrhash zookeeper-browser--marks)
  (zookeeper-browser-render))

(defun zookeeper-browser-visible-paths ()
  "Return all paths currently displayed."
  (let (paths (pos (point-min)))
    (while (and pos (< pos (point-max)))
      (when-let* ((p (get-text-property pos 'zookeeper-path)))
        (unless (member p paths) (push p paths)))
      (setq pos (next-single-property-change pos 'zookeeper-path)))
    (nreverse paths)))

(defun zookeeper-browser-mark-regexp (regexp)
  "Mark visible nodes whose path matches REGEXP."
  (interactive "sMark paths matching regexp: ")
  (let ((n 0))
    (dolist (p (zookeeper-browser-visible-paths))
      (when (string-match-p regexp p)
        (puthash p ?* zookeeper-browser--marks)
        (cl-incf n)))
    (zookeeper-browser-render)
    (message "Marked %d node%s" n (if (= n 1) "" "s"))))

(defun zookeeper-browser-marked (&optional char)
  "Return paths marked with CHAR (default `*'), in display order."
  (let ((c (or char ?*)))
    (sort (cl-loop for p being the hash-keys of zookeeper-browser--marks
                   using (hash-values v) when (eq v c) collect p)
          #'string<)))

(defun zookeeper-browser-targets ()
  "Return the marked paths, or the path at point."
  (or (zookeeper-browser-marked)
      (list (or (zookeeper-browser-path-at-point) (user-error "No node here")))))

(defun zookeeper-browser-execute ()
  "Delete nodes flagged with D."
  (interactive)
  (let ((paths (zookeeper-browser-marked ?D)))
    (unless paths (user-error "No nodes flagged for deletion"))
    (zookeeper-browser--delete-paths paths)
    (dolist (p paths) (remhash p zookeeper-browser--marks))
    (zookeeper-browser-refresh)))

;;;; Operations

(defun zookeeper-browser--refresh-parent-of (path)
  "Refresh the expanded parent of PATH."
  (let ((parent (zookeeper-path-parent path)))
    (puthash parent t zookeeper-browser--expanded)
    (ignore-errors (zookeeper-browser--fetch-sync parent))
    (zookeeper-browser-render)))

(defun zookeeper-browser--base-dir ()
  "Return the path to use as parent for new nodes."
  (let ((p (or (zookeeper-browser-path-at-point) "/")))
    p))

(defconst zookeeper-create-modes
  '("persistent" "persistent-sequential" "ephemeral" "ephemeral-sequential"
    "container" "persistent-with-ttl" "persistent-sequential-with-ttl")
  "Node create modes.")

(defun zookeeper-browser-create (path data mode)
  "Create node PATH with DATA (text) and MODE."
  (interactive
   (progn
     (zookeeper-check-writable zookeeper-cluster)
     (let* ((base (zookeeper-browser--base-dir))
            (path (read-string "Create node: "
                               (file-name-as-directory base)))
            (data (read-string "Data (text): "))
            (mode (completing-read "Mode: " zookeeper-create-modes nil t nil nil
                                   "persistent")))
       (list path data mode))))
  (zookeeper-check-writable zookeeper-cluster)
  (let* ((ttl (and (string-match-p "ttl" mode) (read-number "TTL (ms): " 60000)))
         (res (zookeeper--with-force
               path (lambda (force)
                      (apply #'zookeeper-call zookeeper-cluster "node.create"
                             :path path :text data :mode mode :create_parents t
                             (append (and ttl (list :ttl_ms ttl))
                                     (and force (list :force t))))))))
    (message "Created %s" (plist-get res :path))
    (zookeeper-browser--refresh-parent-of (plist-get res :path))
    (zookeeper-browser--goto-line-of (plist-get res :path))))

(defun zookeeper--with-force (what fn)
  "Call FN with nil; on `policy-denied' offer a forced retry for WHAT."
  (condition-case err (funcall fn nil)
    (zookeeper-error
     (if (and (equal (zookeeper-error-kind err) "policy-denied")
              (not (string-match-p "read.only" (or (zookeeper-error-message err) "")))
              (zookeeper-confirm-force what))
         (funcall fn t)
       (signal (car err) (cdr err))))))

(defun zookeeper-browser-edit ()
  "Edit the node at point."
  (interactive)
  (zookeeper-browser-open-node
   (or (zookeeper-browser-path-at-point) (user-error "No node here"))))

(defun zookeeper-browser-acl ()
  "Show the ACL of the node at point."
  (interactive)
  (require 'zookeeper-acl)
  (zookeeper-acl-open zookeeper-cluster
                      (or (zookeeper-browser-path-at-point) (user-error "No node here"))))

(defun zookeeper-browser-recipe ()
  "Inspect the node at point as a recipe structure."
  (interactive)
  (require 'zookeeper-recipe)
  (zookeeper-recipe-inspect zookeeper-cluster
                            (or (zookeeper-browser-path-at-point) (user-error "No node here"))))

(defun zookeeper-browser-watch ()
  "Add a watch on the node at point and show the events buffer."
  (interactive)
  (require 'zookeeper-watch)
  (call-interactively #'zookeeper-watch-add))

(defun zookeeper-delete-preview-buffer (cluster paths blocked)
  "Show a dry-run preview of PATHS to be deleted on CLUSTER.
BLOCKED is an optional explanation from the bridge."
  (with-current-buffer (get-buffer-create "*zookeeper delete preview*")
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (propertize (format "Dry run on %s: %d node(s) would be deleted\n"
                                  cluster (length paths))
                          'face 'zookeeper-warning-banner))
      (when blocked
        (insert (propertize (format "Blocked: %s\n" blocked) 'face 'zookeeper-error)))
      (insert "\n")
      (dolist (p paths) (insert p "\n"))
      (goto-char (point-min))
      (special-mode))
    (display-buffer (current-buffer))
    (current-buffer)))

(defun zookeeper-browser--delete-paths (paths)
  "Delete PATHS on the current cluster with dry-run preview and confirmation."
  (zookeeper-check-writable zookeeper-cluster)
  (let* ((cluster zookeeper-cluster)
         (previews (mapcar (lambda (p)
                             (zookeeper-call cluster "node.delete" :path p
                                             :recursive t :dry_run t))
                           paths))
         (all (apply #'append (mapcar (lambda (r) (plist-get r :paths)) previews)))
         (blocked (delq nil (mapcar (lambda (r) (plist-get r :blocked)) previews)))
         (protected (cl-some #'zookeeper-browser--protected-p all))
         (recursive (> (length all) (length paths)))
         (preview (zookeeper-delete-preview-buffer
                   cluster all (and blocked (mapconcat (lambda (b) (format "%s" b))
                                                       blocked "; ")))))
    (unwind-protect
        (progn
          (if (or protected blocked recursive
                  (> (length all) zookeeper-browser-large-delete-threshold))
              (zookeeper-confirm-typed
               (format "Delete %d node(s)%s%s on %s?" (length all)
                       (if recursive " recursively" "")
                       (if (or protected blocked) " INCLUDING PROTECTED PATHS" "")
                       cluster))
            (unless (y-or-n-p (format "Delete %s on %s? "
                                      (if (cdr all) (format "%d nodes" (length all))
                                        (car all))
                                      cluster))
              (user-error "Cancelled")))
          (let ((force (and (or protected blocked)
                            (zookeeper-confirm-force (format "%d node(s)" (length all))))))
            (dolist (p paths)
              (apply #'zookeeper-call cluster "node.delete" :path p :recursive t
                     (and force (list :force t))))
            (message "Deleted %d node(s)" (length all))))
      (when (buffer-live-p preview)
        (quit-windows-on preview t)))))

(defun zookeeper-browser-delete ()
  "Delete the marked nodes or the node at point (recursive, with preview)."
  (interactive)
  (let ((paths (zookeeper-browser-targets)))
    (zookeeper-browser--delete-paths paths)
    (dolist (p paths) (remhash p zookeeper-browser--marks))
    (zookeeper-browser-refresh)))

(defun zookeeper-browser-copy (to &optional to-cluster)
  "Copy the subtree at point to path TO, possibly on TO-CLUSTER."
  (interactive
   (let* ((p (or (zookeeper-browser-path-at-point) (user-error "No node here")))
          (tc (if current-prefix-arg (zookeeper-read-cluster "Target cluster: ")
                zookeeper-cluster)))
     (list (read-string (format "Copy %s to: " p) p) tc)))
  (let ((from (zookeeper-browser-path-at-point))
        (tc (or to-cluster zookeeper-cluster)))
    (zookeeper-check-writable tc)
    (let ((res (apply #'zookeeper-call zookeeper-cluster "node.copy" :path from :to to
                      (and (not (equal tc zookeeper-cluster))
                           (list :to_cluster (zookeeper-cluster-id tc))))))
      (message "Copied %s nodes" (plist-get res :created))
      (when (equal tc zookeeper-cluster) (zookeeper-browser--refresh-parent-of to)))))

(defun zookeeper-browser-rename (to)
  "Move the node at point to TO."
  (interactive
   (let ((p (or (zookeeper-browser-path-at-point) (user-error "No node here"))))
     (zookeeper-check-writable zookeeper-cluster)
     (list (read-string (format "Move %s to: " p) p))))
  (let ((from (zookeeper-browser-path-at-point)))
    (zookeeper-check-writable zookeeper-cluster)
    (condition-case err
        (zookeeper-call zookeeper-cluster "node.move" :path from :to to)
      (zookeeper-error
       (if (and (member (zookeeper-error-kind err) '("limit-exceeded" "bad-arguments"))
                (yes-or-no-p (format "%s.  Move non-atomically? "
                                     (zookeeper-error-message err))))
           (zookeeper-call zookeeper-cluster "node.move" :path from :to to
                           :allow_non_atomic t)
         (signal (car err) (cdr err)))))
    (zookeeper-browser--refresh-parent-of from)
    (zookeeper-browser--refresh-parent-of to)
    (message "Moved %s -> %s" from to)))

;;;; Goto with completion

(defun zookeeper--path-completion-table (cluster)
  "Return a completion table over node paths of CLUSTER."
  (let ((cache (make-hash-table :test 'equal)))
    (lambda (string pred action)
      (if (eq action 'metadata)
          '(metadata (category . zookeeper-path))
        (let* ((dir (if (string-match "\\`\\(.*\\)/[^/]*\\'" string)
                        (let ((d (match-string 1 string))) (if (equal d "") "/" d))
                      "/"))
               (names (or (gethash dir cache)
                          (puthash dir
                                   (condition-case nil
                                       (mapcar (lambda (c) (zookeeper-path-join
                                                            dir (plist-get c :name)))
                                               (plist-get (zookeeper-call
                                                           cluster "node.children"
                                                           :path dir :limit 1000)
                                                          :children))
                                     (error nil))
                                   cache))))
          (complete-with-action action names string pred))))))

(defun zookeeper-read-path (cluster prompt &optional initial)
  "Read a node path of CLUSTER with PROMPT and INITIAL input."
  (completing-read prompt (zookeeper--path-completion-table cluster)
                   nil nil (or initial "/")))

(defun zookeeper-goto-path (path)
  "Reveal PATH in the browser."
  (interactive (list (zookeeper-read-path (zookeeper-current-cluster) "Go to path: "
                                          (zookeeper-browser-path-at-point))))
  (if (derived-mode-p 'zookeeper-browser-mode)
      (zookeeper-browser-reveal (directory-file-name (if (equal path "/") "//" path)))
    (zookeeper-browse (zookeeper-current-cluster) path)))

(defun zookeeper-visit-recent ()
  "Visit a recently opened node."
  (interactive)
  (unless zookeeper-recent-paths (user-error "No recent paths"))
  (let* ((cands (mapcar (lambda (e) (cons (format "%s:%s" (car e) (cdr e)) e))
                        zookeeper-recent-paths))
         (e (cdr (assoc (completing-read "Recent node: " cands nil t) cands))))
    (require 'zookeeper-node)
    (zookeeper-node-open (car e) (cdr e))))

;;;; Search

(defvar-local zookeeper-search--params nil "Params of the search shown.")

(define-derived-mode zookeeper-search-mode tabulated-list-mode "ZK-Search"
  "Results of a ZooKeeper node search."
  (setq tabulated-list-format [("Path" 60 t) ("Reasons" 18 nil) ("Size" 8 nil)
                               ("Modified" 19 t)])
  (setq tabulated-list-padding 1)
  (zookeeper-setup-mode-line)
  (tabulated-list-init-header))

(define-key zookeeper-search-mode-map (kbd "RET") #'zookeeper-search-visit)
(define-key zookeeper-search-mode-map "w" #'zookeeper-search-copy-path)

(defun zookeeper-search (cluster path name-re data-re)
  "Search CLUSTER below PATH for names matching NAME-RE / data DATA-RE."
  (interactive
   (let ((c (zookeeper-current-cluster)))
     (list c
           (zookeeper-read-path c "Search below: "
                                (or (and (derived-mode-p 'zookeeper-browser-mode)
                                         (zookeeper-browser-path-at-point))
                                    "/"))
           (read-string "Name regex (empty = any): ")
           (read-string "Data regex (empty = any): "))))
  (let* ((params (append (list :path path)
                         (and (not (string-empty-p name-re)) (list :name name-re))
                         (and (not (string-empty-p data-re)) (list :data data-re))))
         (res (progn (message "Searching...")
                     (apply #'zookeeper-call cluster "node.search" params))))
    (with-current-buffer (get-buffer-create (format "*zookeeper search: %s*" cluster))
      (zookeeper-search-mode)
      (setq zookeeper-cluster cluster zookeeper-search--params params)
      (setq tabulated-list-entries
            (mapcar (lambda (m)
                      (let ((st (plist-get m :stat)))
                        (list (plist-get m :path)
                              (vector (propertize (plist-get m :path) 'face 'zookeeper-path)
                                      (string-join (plist-get m :reasons) ",")
                                      (zookeeper-format-size (plist-get st :data_length))
                                      (zookeeper-format-time (plist-get st :mtime))))))
                    (plist-get res :matches)))
      (tabulated-list-print)
      (setq header-line-format
            (format " %d match(es), %s scanned%s%s"
                    (length (plist-get res :matches)) (plist-get res :scanned)
                    (if (zookeeper-true-p (plist-get res :truncated)) ", TRUNCATED" "")
                    (if (plist-get res :partial_reasons)
                        (format ", partial: %s"
                                (string-join (plist-get res :partial_reasons) " "))
                      "")))
      (pop-to-buffer (current-buffer)))))

(defun zookeeper-search-visit ()
  "Open the search result at point."
  (interactive)
  (require 'zookeeper-node)
  (when-let* ((p (tabulated-list-get-id)))
    (zookeeper-node-open zookeeper-cluster p)))

(defun zookeeper-search-copy-path ()
  "Copy path of result at point."
  (interactive)
  (when-let* ((p (tabulated-list-get-id))) (kill-new p) (message "%s" p)))

;;;; Diff

(defun zookeeper-diff (left-cluster left-path right-cluster right-path)
  "Diff LEFT-CLUSTER:LEFT-PATH against RIGHT-CLUSTER:RIGHT-PATH subtrees."
  (interactive
   (let* ((lc (zookeeper-current-cluster))
          (marked (and (derived-mode-p 'zookeeper-browser-mode)
                       (zookeeper-browser-marked)))
          (lp (or (car marked)
                  (zookeeper-read-path lc "Left path: "
                                       (and (derived-mode-p 'zookeeper-browser-mode)
                                            (zookeeper-browser-path-at-point)))))
          (rc (if current-prefix-arg (zookeeper-read-cluster "Right cluster: ") lc))
          (rp (or (and (equal rc lc) (cadr marked))
                  (zookeeper-read-path rc "Right path: " lp))))
     (list lc lp rc rp)))
  (let* ((res (zookeeper-rpc "node.diff"
                             (list :left (list :cluster (zookeeper-cluster-id left-cluster)
                                               :path left-path)
                                   :right (list :cluster (zookeeper-cluster-id right-cluster)
                                                :path right-path)
                                   :data t :acl t)
                             120)))
    (with-current-buffer (get-buffer-create "*zookeeper diff*")
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (zookeeper-diff-format res left-cluster left-path right-cluster right-path))
        (goto-char (point-min))
        (special-mode))
      (pop-to-buffer (current-buffer)))))

(defun zookeeper--b64-text (s)
  "Decode base64 S to a display string."
  (if (null s) ""
    (let ((bytes (condition-case nil (base64-decode-string s) (error s))))
      (condition-case nil
          (decode-coding-string bytes 'utf-8 t)
        (error bytes)))))

(defun zookeeper-diff-format (res lc lp rc rp)
  "Format node.diff RES for LC:LP vs RC:RP as a string."
  (with-temp-buffer
    (insert (propertize (format "--- %s:%s (%s nodes)\n+++ %s:%s (%s nodes)\n"
                                lc lp (plist-get res :left_count)
                                rc rp (plist-get res :right_count))
                        'face 'bold))
    (when (zookeeper-true-p (plist-get res :truncated))
      (insert (propertize "TRUNCATED: comparison incomplete\n" 'face 'zookeeper-error)))
    (if (null (plist-get res :differences))
        (insert "\nNo differences.\n")
      (dolist (d (plist-get res :differences))
        (let ((kind (plist-get d :kind)) (rel (plist-get d :rel)))
          (insert "\n"
                  (propertize (format "%-12s %s" kind (if (equal rel "") "." rel))
                              'face (pcase kind
                                      ("only-left" 'diff-removed)
                                      ("only-right" 'diff-added)
                                      (_ 'diff-changed)))
                  "\n")
          (when (equal kind "data")
            (insert (propertize (concat "- " (zookeeper--b64-text
                                              (plist-get (plist-get d :left) :data))
                                        "\n")
                                'face 'diff-removed)
                    (propertize (concat "+ " (zookeeper--b64-text
                                              (plist-get (plist-get d :right) :data))
                                        "\n")
                                'face 'diff-added))))))
    (buffer-string)))

(defun zookeeper-browser-diff ()
  "Diff two marked nodes, or the node at point against another path."
  (interactive)
  (call-interactively #'zookeeper-diff))

;;;; Bookmarks

(defun zookeeper-browser-bookmark-make-record ()
  "Return a bookmark record for the node at point."
  `(,(format "zk %s:%s" zookeeper-cluster (or (zookeeper-browser-path-at-point) "/"))
    (cluster . ,zookeeper-cluster)
    (zk-path . ,(or (zookeeper-browser-path-at-point) "/"))
    (handler . zookeeper-bookmark-jump)))

;;;###autoload
(defun zookeeper-bookmark-jump (bookmark)
  "Jump to ZooKeeper BOOKMARK."
  (let ((buf (save-window-excursion
               (zookeeper-browse (bookmark-prop-get bookmark 'cluster)
                                 (bookmark-prop-get bookmark 'zk-path)))))
    (bookmark-default-handler `("" (buffer . ,buf) . ,(bookmark-get-bookmark-record bookmark)))))

;;;; Embark

(defun zookeeper-embark-target ()
  "Return the path at point as an Embark target."
  (when-let* ((p (and (derived-mode-p 'zookeeper-browser-mode)
                     (zookeeper-browser-path-at-point))))
    `(zookeeper-path ,p ,(line-beginning-position) . ,(line-end-position))))

(defvar embark-target-finders)
(defvar embark-keymap-alist)
(with-eval-after-load 'embark
  (add-to-list 'embark-target-finders #'zookeeper-embark-target)
  (defvar zookeeper-embark-path-map
    (let ((m (make-sparse-keymap)))
      (define-key m "w" #'kill-new)
      (define-key m "j" #'zookeeper-goto-path)
      m)
    "Embark actions for `zookeeper-path' targets.")
  (add-to-list 'embark-keymap-alist '(zookeeper-path . zookeeper-embark-path-map)))

(provide 'zookeeper-browser)
;;; zookeeper-browser.el ends here
