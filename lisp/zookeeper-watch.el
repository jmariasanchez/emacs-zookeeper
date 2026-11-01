;;; zookeeper-watch.el --- Event log and watches  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; The *zookeeper events* buffer logs watch, connection, cache and
;; audit notifications as locally observed by this bridge.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'tabulated-list)
(require 'zookeeper-connection)

(defcustom zookeeper-events-max 5000
  "Maximum number of events kept in the event log."
  :type 'integer :group 'zookeeper)

(defcustom zookeeper-events-mode-hook nil
  "Hook run when entering `zookeeper-events-mode'."
  :type 'hook :group 'zookeeper)

(defvar zookeeper-events nil
  "Locally observed events, newest first.  Each is a plist.")

(defvar zookeeper-events-filter nil
  "When non-nil, a regexp events must match to be shown.")

(defvar zookeeper-events-follow t
  "When non-nil, the events buffer keeps point at the newest event.")

(defconst zookeeper-events-buffer-name "*zookeeper events*"
  "Name of the event log buffer.")

(defun zookeeper-events--summarize (type params)
  "Return (CLUSTER KIND PATH DETAIL) for notification TYPE with PARAMS."
  (pcase type
    ('watch (let ((ev (plist-get params :event)))
              (list (zookeeper--name-for-id (plist-get params :cluster))
                    (if (plist-get params :removed) "watch-removed" (plist-get ev :kind))
                    (or (plist-get ev :path) "")
                    (format "watch %s%s" (plist-get params :watch)
                            (if (plist-get ev :dropped)
                                (format " dropped %s" (plist-get ev :dropped)) "")))))
    ('connection (let ((ev (plist-get params :event)))
                   (list (or (zookeeper--name-for-id (plist-get params :cluster))
                             (plist-get params :cluster))
                         (concat "conn:" (or (plist-get ev :kind) ""))
                         "" (or (plist-get ev :message)
                             (zookeeper--state-string (plist-get params :state)) ""))))
    ('cache (let ((ev (plist-get params :event)))
              (list nil (format "cache:%s" (or (and (listp ev) (or (plist-get ev :type)
                                                                   (plist-get ev :kind)))
                                               ev))
                    (or (and (listp ev) (plist-get ev :path)) "")
                    (format "cache %s" (plist-get params :cache)))))
    ('audit (list (plist-get params :cluster)
                  (concat "audit:" (or (plist-get params :operation) ""))
                  (string-join (mapcar (lambda (p) (format "%s" p))
                                       (plist-get params :paths))
                               " ")
                  (concat (format "%s" (plist-get params :outcome))
                          (if (zookeeper-true-p (plist-get params :forced)) " FORCED" ""))))
    ('recipe (list nil (concat "recipe:" (or (plist-get params :state) ""))
                   "" (format "recipe %s" (plist-get params :recipe))))))

(defun zookeeper-events-record (type params)
  "Record notification of TYPE with PARAMS in the event log."
  (let ((e (append (list :time (current-time) :type type)
                   (cl-mapcan #'list '(:cluster :kind :path :detail)
                              (zookeeper-events--summarize type params)))))
    (push e zookeeper-events)
    (when (> (length zookeeper-events) zookeeper-events-max)
      (setcdr (nthcdr (1- zookeeper-events-max) zookeeper-events) nil))
    (when-let* ((buf (get-buffer zookeeper-events-buffer-name)))
      (with-current-buffer buf (zookeeper-events--print)))))

(add-hook 'zookeeper-watch-event-functions (lambda (p) (zookeeper-events-record 'watch p)))
(add-hook 'zookeeper-connection-event-functions
          (lambda (p) (zookeeper-events-record 'connection p)))
(add-hook 'zookeeper-cache-event-functions (lambda (p) (zookeeper-events-record 'cache p)))
(add-hook 'zookeeper-audit-functions (lambda (p) (zookeeper-events-record 'audit p)))
(add-hook 'zookeeper-recipe-ownership-functions
          (lambda (p) (zookeeper-events-record 'recipe p)))

(defun zookeeper-events--line (e)
  "Return the searchable text of event E."
  (format "%s %s %s %s" (plist-get e :cluster) (plist-get e :kind)
          (plist-get e :path) (plist-get e :detail)))

(defun zookeeper-events-visible ()
  "Return events passing `zookeeper-events-filter', oldest first."
  (reverse (if zookeeper-events-filter
               (cl-remove-if-not (lambda (e) (string-match-p zookeeper-events-filter
                                                             (zookeeper-events--line e)))
                                 zookeeper-events)
             zookeeper-events)))

(defvar zookeeper-events-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m "/" #'zookeeper-events-set-filter)
    (define-key m "f" #'zookeeper-events-toggle-follow)
    (define-key m "C" #'zookeeper-events-clear)
    (define-key m "+" #'zookeeper-watch-add)
    (define-key m "-" #'zookeeper-watch-remove)
    (define-key m "L" #'zookeeper-watch-list)
    m)
  "Keymap for `zookeeper-events-mode'.")

(define-derived-mode zookeeper-events-mode tabulated-list-mode "ZK-Events"
  "Log of locally observed ZooKeeper events.
\\{zookeeper-events-mode-map}"
  (setq tabulated-list-format [("Time" 12 nil) ("Cluster" 10 nil) ("Kind" 24 nil)
                               ("Path" 40 nil) ("Detail" 30 nil)])
  (setq tabulated-list-padding 1)
  (tabulated-list-init-header))

(defun zookeeper-events--print ()
  "Redisplay the event log."
  (setq tabulated-list-entries
        (mapcar (lambda (e)
                  (list e (vector (format-time-string "%H:%M:%S.%3N" (plist-get e :time))
                                  (format "%s" (or (plist-get e :cluster) ""))
                                  (propertize (format "%s" (plist-get e :kind))
                                              'face (if (string-match-p
                                                         "lost\\|closed\\|resync\\|overflow\\|error\\|stale"
                                                         (format "%s" (plist-get e :kind)))
                                                        'zookeeper-error 'zookeeper-path))
                                  (format "%s" (plist-get e :path))
                                  (format "%s" (plist-get e :detail)))))
                (zookeeper-events-visible)))
  (setq header-line-format
        (format " Locally observed events (not a server history)%s%s"
                (if zookeeper-events-filter (format "  filter: %s" zookeeper-events-filter) "")
                (if zookeeper-events-follow "  [follow]" "")))
  (tabulated-list-print t)
  (when zookeeper-events-follow
    (goto-char (point-max))
    (dolist (w (get-buffer-window-list (current-buffer) nil t))
      (set-window-point w (point-max)))))

;;;###autoload
(defun zookeeper-events ()
  "Show the event log."
  (interactive)
  (with-current-buffer (get-buffer-create zookeeper-events-buffer-name)
    (unless (derived-mode-p 'zookeeper-events-mode) (zookeeper-events-mode))
    (zookeeper-events--print)
    (pop-to-buffer (current-buffer))))

(defun zookeeper-events-set-filter (regexp)
  "Show only events matching REGEXP (empty clears)."
  (interactive "sFilter events (regexp): ")
  (setq zookeeper-events-filter (if (string-empty-p regexp) nil regexp))
  (zookeeper-events--print))

(defun zookeeper-events-toggle-follow ()
  "Toggle follow mode."
  (interactive)
  (setq zookeeper-events-follow (not zookeeper-events-follow))
  (zookeeper-events--print))

(defun zookeeper-events-clear ()
  "Clear the event log."
  (interactive)
  (setq zookeeper-events nil)
  (zookeeper-events--print))

(defconst zookeeper-watch-kinds '("data" "exists" "children" "persistent" "recursive")
  "Watch kinds.")

(declare-function zookeeper-browser-path-at-point "zookeeper-browser")
(declare-function zookeeper-read-path "zookeeper-browser")

(defun zookeeper-watch-add (cluster path kind)
  "Add a watch of KIND on PATH of CLUSTER."
  (interactive
   (let ((c (zookeeper-current-cluster)))
     (require 'zookeeper-browser)
     (list c (zookeeper-read-path c "Watch path: "
                                  (and (derived-mode-p 'zookeeper-browser-mode)
                                       (zookeeper-browser-path-at-point)))
           (completing-read "Kind: " zookeeper-watch-kinds nil t nil nil "persistent"))))
  (let ((res (zookeeper-call cluster "watch.add" :path path :kind kind)))
    (message "Watch %s on %s (%s)" (plist-get res :watch) path kind)
    (when (called-interactively-p 'interactive)
      (save-selected-window (zookeeper-events)))
    (plist-get res :watch)))

(defun zookeeper-watch--list (&optional cluster)
  "Return watches, of CLUSTER if non-nil."
  (zookeeper-rpc "watch.list" (if cluster (list :cluster (zookeeper-cluster-id cluster))
                                (make-hash-table))))

(defun zookeeper-watch-remove (watch)
  "Remove WATCH id."
  (interactive
   (let* ((ws (zookeeper-watch--list))
          (cands (mapcar (lambda (w) (cons (format "%s %s %s" (plist-get w :watch)
                                                   (plist-get w :kind) (plist-get w :path))
                                           (plist-get w :watch)))
                         ws)))
     (unless cands (user-error "No watches"))
     (list (cdr (assoc (completing-read "Remove watch: " cands nil t) cands)))))
  (zookeeper-rpc "watch.remove" (list :watch watch))
  (message "Removed watch %s" watch))

(defun zookeeper-watch-list ()
  "Display active watches."
  (interactive)
  (let ((ws (zookeeper-watch--list)))
    (with-current-buffer (get-buffer-create "*zookeeper watches*")
      (tabulated-list-mode)
      (setq tabulated-list-format [("Id" 8 t) ("Cluster" 10 t) ("Kind" 12 t)
                                   ("Events" 7 t) ("Path" 50 t)])
      (setq tabulated-list-entries
            (mapcar (lambda (w)
                      (list (plist-get w :watch)
                            (vector (format "%s" (plist-get w :watch))
                                    (format "%s" (or (zookeeper--name-for-id (plist-get w :cluster))
                                                     (plist-get w :cluster)))
                                    (format "%s" (plist-get w :kind))
                                    (format "%s" (or (plist-get w :events) 0))
                                    (format "%s" (plist-get w :path)))))
                    ws))
      (tabulated-list-init-header)
      (tabulated-list-print)
      (pop-to-buffer (current-buffer)))))

(provide 'zookeeper-watch)
;;; zookeeper-watch.el ends here
