;;; zookeeper-recipe.el --- Recipe inspection and controlled recipes  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Read-only inspection of lock/election/semaphore/queue/discovery
;; structures, plus controlled lock/latch/member participants owned by
;; the bridge, with their ownership state displayed prominently.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'zookeeper-connection)

(defvar zookeeper-recipe--handles (make-hash-table :test 'equal)
  "Recipe id -> plist (:kind :cluster :path :state :leader).")

(defun zookeeper-recipe--call (method params)
  "Call recipe METHOD with PARAMS, translating unsupported methods."
  (condition-case err (zookeeper-rpc method params)
    (zookeeper-error
     (if (equal (zookeeper-error-kind err) "method-not-found")
         (user-error "%s is not supported by this bridge" method)
       (signal (car err) (cdr err))))))

(defun zookeeper-recipe--pp (value indent)
  "Insert VALUE (parsed JSON) pretty-printed at INDENT."
  (let ((pad (make-string indent ?\s)))
    (cond
     ((and (listp value) (keywordp (car-safe value)))
      (cl-loop for (k v) on value by #'cddr
               do (if (and (listp v) v)
                      (progn (insert pad (propertize (substring (symbol-name k) 1)
                                                     'face 'bold)
                                     ":\n")
                             (zookeeper-recipe--pp v (+ indent 2)))
                    (insert pad (propertize (substring (symbol-name k) 1) 'face 'bold)
                            ": " (zookeeper-recipe--scalar k v) "\n"))))
     ((listp value)
      (dolist (v value)
        (if (and (listp v) v)
            (progn (insert pad "-\n") (zookeeper-recipe--pp v (+ indent 2)))
          (insert pad "- " (format "%s" v) "\n"))))
     (t (insert pad (format "%s" value) "\n")))))

(defun zookeeper-recipe--scalar (key value)
  "Format scalar VALUE of KEY."
  (cond ((and (memq key '(:ephemeral_owner :session :owner)) (integerp value))
         (or (zookeeper-format-session value) "0"))
        ((and (integerp value) (string-match-p "_ms\\'\\|time" (symbol-name key))
              (> value 1000000000000))
         (zookeeper-format-time value))
        ((eq value :false) "false")
        ((eq value t) "true")
        (t (format "%s" value))))

;;;###autoload
(defun zookeeper-recipe-inspect (cluster path &optional kind)
  "Inspect recipe structure at PATH of CLUSTER, as KIND (default auto)."
  (interactive
   (let ((c (zookeeper-current-cluster)))
     (list c (read-string "Recipe path: " "/")
           (completing-read "Kind: " '("auto" "lock" "election" "semaphore" "queue"
                                       "discovery" "barrier" "counter")
                            nil t nil nil "auto"))))
  (let ((res (zookeeper-recipe--call "recipe.inspect"
                                     (list :cluster (zookeeper-cluster-id cluster)
                                           :path path :kind (or kind "auto")))))
    (with-current-buffer (get-buffer-create (format "*zk recipe %s:%s*" cluster path))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Recipe at %s:%s (read-only inspection)\n\n" cluster path)
                            'face 'zookeeper-path))
        (zookeeper-recipe--pp res 0))
      (special-mode)
      (setq zookeeper-cluster cluster)
      (local-set-key "g" (lambda () (interactive) (zookeeper-recipe-inspect cluster path kind)))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer)))))

(defun zookeeper-discovery-browse (cluster base)
  "Browse service discovery under BASE on CLUSTER."
  (interactive (list (zookeeper-current-cluster) (read-string "Discovery base: " "/services")))
  (let* ((id (zookeeper-cluster-id cluster))
         (services (zookeeper-recipe--call "recipe.discovery.services"
                                           (list :cluster id :base base))))
    (with-current-buffer (get-buffer-create (format "*zk discovery %s:%s*" cluster base))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Services under %s:%s\n\n" cluster base) 'face 'bold))
        (dolist (s services)
          (insert (propertize s 'face 'zookeeper-path) "\n")
          (dolist (inst (zookeeper-recipe--call "recipe.discovery.instances"
                                                (list :cluster id :base base :service s)))
            (zookeeper-recipe--pp inst 4))))
      (special-mode)
      (pop-to-buffer (current-buffer)))))

(defun zookeeper-counter-view (cluster path &optional encoding)
  "Show the counter at PATH on CLUSTER with ENCODING."
  (interactive (list (zookeeper-current-cluster) (read-string "Counter path: " "/")
                     (completing-read "Encoding: " '("i64" "i32" "text") nil t nil nil "i64")))
  (let ((res (zookeeper-recipe--call "recipe.counter.get"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path
                                           :encoding (or encoding "i64")))))
    (message "%s:%s = %s (version %s)" cluster path (plist-get res :value)
             (plist-get res :version))
    res))

(defun zookeeper-counter-add (cluster path delta)
  "Add DELTA to the counter at PATH on CLUSTER."
  (interactive (list (zookeeper-current-cluster) (read-string "Counter path: " "/")
                     (read-number "Delta: " 1)))
  (zookeeper-check-writable cluster)
  (when (y-or-n-p (format "Add %s to counter %s? " delta path))
    (let ((res (zookeeper-recipe--call "recipe.counter.add"
                                       (list :cluster (zookeeper-cluster-id cluster)
                                             :path path :delta delta))))
      (message "%s = %s" path (plist-get res :value)))))

;;;; Controlled recipes

(defun zookeeper-recipe--warn (what cluster path)
  "Ask for typed confirmation before starting WHAT at PATH on CLUSTER."
  (zookeeper-check-writable cluster)
  (zookeeper-confirm-typed
   (format "%s at %s:%s takes part in a live coordination protocol and may affect production processes."
           what cluster path)))

(defun zookeeper-recipe--register (res kind cluster path)
  "Register recipe handle from RES of KIND on CLUSTER PATH."
  (puthash (plist-get res :recipe)
           (list :kind kind :cluster cluster :path path
                 :state (or (plist-get res :ownership) "held"))
           zookeeper-recipe--handles)
  (zookeeper-recipes-refresh)
  (plist-get res :recipe))

(defun zookeeper-lock-acquire (cluster path timeout-ms)
  "Acquire a lock at PATH on CLUSTER within TIMEOUT-MS."
  (interactive (list (zookeeper-current-cluster) (read-string "Lock path: " "/")
                     (read-number "Timeout (ms): " 5000)))
  (zookeeper-recipe--warn "Acquiring a lock" cluster path)
  (let ((res (zookeeper-recipe--call "recipe.lock.acquire"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path
                                           :timeout_ms timeout-ms))))
    (zookeeper-recipe--register res "lock" cluster path)))

(defun zookeeper-latch-start (cluster path)
  "Join leader latch at PATH on CLUSTER."
  (interactive (list (zookeeper-current-cluster) (read-string "Latch path: " "/")))
  (zookeeper-recipe--warn "Joining a leader election" cluster path)
  (zookeeper-recipe--register
   (zookeeper-recipe--call "recipe.latch.start"
                           (list :cluster (zookeeper-cluster-id cluster) :path path))
   "latch" cluster path))

(defun zookeeper-member-join (cluster path member-id)
  "Join group membership at PATH on CLUSTER as MEMBER-ID."
  (interactive (list (zookeeper-current-cluster) (read-string "Group path: " "/")
                     (read-string "Member id: " (system-name))))
  (zookeeper-recipe--warn "Joining a group" cluster path)
  (zookeeper-recipe--register
   (zookeeper-recipe--call "recipe.member.join"
                           (list :cluster (zookeeper-cluster-id cluster) :path path
                                 :member_id member-id))
   "member" cluster path))

(defun zookeeper-recipe-release (recipe)
  "Release RECIPE handle."
  (interactive
   (list (or (and (derived-mode-p 'zookeeper-recipes-mode) (tabulated-list-get-id))
             (completing-read "Release recipe: "
                              (hash-table-keys zookeeper-recipe--handles) nil t))))
  (zookeeper-recipe--call "recipe.release" (list :recipe recipe))
  (remhash recipe zookeeper-recipe--handles)
  (zookeeper-recipes-refresh)
  (message "Released %s" recipe))

(defun zookeeper-recipe--on-ownership (params)
  "Update recipe state from ownership notification PARAMS."
  (when-let* ((h (gethash (plist-get params :recipe) zookeeper-recipe--handles)))
    (setq h (plist-put h :state (plist-get params :state)))
    (setq h (plist-put h :leader (plist-get params :leader)))
    (puthash (plist-get params :recipe) h zookeeper-recipe--handles)
    (when (member (plist-get params :state) '("lost" "suspended"))
      (message "WARNING: recipe %s on %s is %s; do not assume ownership"
               (plist-get params :recipe) (plist-get h :path) (plist-get params :state)))
    (zookeeper-recipes-refresh)))

(add-hook 'zookeeper-recipe-ownership-functions #'zookeeper-recipe--on-ownership)

(defvar zookeeper-recipes-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m "k" #'zookeeper-recipe-release)
    (define-key m "l" #'zookeeper-lock-acquire)
    (define-key m "L" #'zookeeper-latch-start)
    (define-key m "j" #'zookeeper-member-join)
    m)
  "Keymap for `zookeeper-recipes-mode'.")

(define-derived-mode zookeeper-recipes-mode tabulated-list-mode "ZK-Recipes"
  "Recipes held by the bridge on behalf of this Emacs.
\\{zookeeper-recipes-mode-map}"
  (setq tabulated-list-format [("Id" 8 t) ("Kind" 8 t) ("Cluster" 10 t)
                               ("State" 12 t) ("Leader" 6 nil) ("Path" 40 t)])
  (tabulated-list-init-header))

(defun zookeeper-recipes-refresh ()
  "Redisplay the recipes buffer if present."
  (when-let* ((b (get-buffer "*zookeeper recipes*")))
    (with-current-buffer b
      (setq tabulated-list-entries
            (cl-loop for id being the hash-keys of zookeeper-recipe--handles
                     using (hash-values h)
                     collect (list id (vector id (plist-get h :kind) (plist-get h :cluster)
                                              (propertize (format "%s" (plist-get h :state))
                                                          'face (if (equal (plist-get h :state) "held")
                                                                    'zookeeper-connected
                                                                  'zookeeper-warning-banner))
                                              (if (zookeeper-true-p (plist-get h :leader)) "yes" "")
                                              (plist-get h :path)))))
      (setq header-line-format
            (propertize " Held by the bridge session.  Ownership is lost if the session is lost."
                        'face 'zookeeper-warning-banner))
      (tabulated-list-print t))))

;;;###autoload
(defun zookeeper-recipes ()
  "Show recipe handles held by the bridge."
  (interactive)
  (with-current-buffer (get-buffer-create "*zookeeper recipes*")
    (zookeeper-recipes-mode)
    (zookeeper-recipes-refresh)
    (pop-to-buffer (current-buffer))))

(provide 'zookeeper-recipe)
;;; zookeeper-recipe.el ends here
