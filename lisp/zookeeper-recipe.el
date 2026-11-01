;;; zookeeper-recipe.el --- Recipe inspection and controlled recipes  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Read-only inspection of lock/election/semaphore/queue/discovery/counter
;; structures (recipe.inspect), a service discovery browser, a counter
;; view, and controlled lock/latch/member participants owned by the
;; bridge, with their live ownership shown prominently.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'tabulated-list)
(require 'zookeeper-connection)

(defvar zookeeper-recipe--handles (make-hash-table :test 'equal)
  "Recipe id -> plist (:kind :cluster :path :state :leader :node).")

(defun zookeeper-recipe--call (method params)
  "Call recipe METHOD with PARAMS, translating unsupported methods."
  (condition-case err (zookeeper-rpc method params)
    (zookeeper-error
     (if (equal (zookeeper-error-kind err) "method-not-found")
         (user-error "%s is not supported by this bridge" method)
       (signal (car err) (cdr err))))))

;;;; Generic pretty printer (also used by diagnostics)

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
  (cond ((and (memq key '(:ephemeral_owner :owner_session)) (integerp value))
         (or (zookeeper-format-session value) "0"))
        ((and (integerp value) (string-match-p "_ms\\'\\|time\\|UTC" (symbol-name key))
              (> value 1000000000000))
         (zookeeper-format-time value))
        ((eq value :false) "false")
        ((eq value t) "true")
        ((null value) "-")
        (t (format "%s" value))))

;;;; Inspection rendering (pure: returns a string)

(defun zookeeper-recipe--b64 (s)
  "Decode base64 S for display, or return S."
  (if (and (stringp s) (not (string-empty-p s)))
      (condition-case nil
          (let ((b (base64-decode-string s)))
            (decode-coding-string b 'utf-8))
        (error s))
    ""))

(defun zookeeper-recipe--participants (title items owner-pred)
  "Insert TITLE and ITEMS (contenders/participants) in sequence order.
OWNER-PRED returns non-nil for the item that holds the recipe."
  (insert (propertize (format "%s (%d)\n" title (length items)) 'face 'bold))
  (if (null items)
      (insert "  (none)\n")
    (dolist (it (sort (copy-sequence items)
                      (lambda (a b) (< (or (plist-get a :sequence) 0)
                                       (or (plist-get b :sequence) 0)))))
      (let ((owner (funcall owner-pred it)))
        (insert (if owner (propertize "  ★ " 'face 'zookeeper-connected) "    ")
                (format "#%-6s " (or (plist-get it :sequence) "?"))
                (propertize (format "%s" (plist-get it :node))
                            'face (if owner 'zookeeper-connected 'zookeeper-path))
                (if-let* ((s (plist-get it :session_id)))
                    (propertize (format "  session %s" s) 'face 'zookeeper-dim) "")
                (let ((d (plist-get it :data)))
                  (if (and d (not (equal d "")))
                      (format "  data=%S" (truncate-string-to-width
                                           (zookeeper-recipe--b64 d) 40 nil nil "…"))
                    ""))
                (if owner (propertize "  OWNER" 'face 'zookeeper-connected) "")
                "\n")))))

(defun zookeeper-recipe--queue-head (head)
  "Insert queue HEAD items."
  (insert (propertize "Head\n" 'face 'bold))
  (if (null head) (insert "  (empty)\n")
    (dolist (it head)
      (insert (format "  #%-6s %s%s%s\n" (or (plist-get it :sequence) "?")
                      (plist-get it :node)
                      (if (plist-get it :priority)
                          (format "  priority=%s" (plist-get it :priority)) "")
                      (if (plist-get it :ready_at_ms)
                          (format "  ready at %s"
                                  (zookeeper-format-time (plist-get it :ready_at_ms)))
                        ""))))))

(defun zookeeper-recipe--instance-line (inst)
  "Return a one-line summary of service instance INST."
  (format "%s  %s:%s%s%s"
          (propertize (format "%s" (plist-get inst :id)) 'face 'zookeeper-path)
          (or (plist-get inst :address) "?") (or (plist-get inst :port) "-")
          (if (plist-get inst :sslPort) (format " ssl:%s" (plist-get inst :sslPort)) "")
          (if-let* ((ts (plist-get inst :registrationTimeUTC)))
              (propertize (format "  registered %s" (zookeeper-format-time ts))
                          'face 'zookeeper-dim)
            "")))

(defun zookeeper-recipe-render (res)
  "Return a display string for recipe.inspect result RES."
  (with-temp-buffer
    (let ((kind (plist-get res :kind)))
      (insert (propertize (format "%s" kind) 'face 'zookeeper-warning-banner)
              "  " (propertize (format "%s" (plist-get res :path)) 'face 'zookeeper-path)
              "\n\n")
      (pcase kind
        ("lock"
         (let ((owner (plist-get res :owner)))
           (insert "Owner: " (if owner (propertize owner 'face 'zookeeper-connected)
                               (propertize "none (free)" 'face 'zookeeper-dim))
                   "\n\n")
           (zookeeper-recipe--participants
            "Contenders" (plist-get res :contenders)
            (lambda (it) (or (zookeeper-true-p (plist-get it :is_owner))
                             (equal (plist-get it :node) owner))))))
        ("read-write-lock"
         (insert (format "Writer holding: %s\nReaders holding: %s\n\n"
                         (zookeeper-recipe--scalar :w (plist-get res :writer_holding))
                         (let ((r (plist-get res :readers_holding)))
                           (if (listp r) (length r) r))))
         (zookeeper-recipe--participants
          "Contenders" (plist-get res :contenders)
          (lambda (it) (zookeeper-true-p (plist-get it :is_owner)))))
        ("leader-latch"
         (let ((leader (plist-get res :leader)))
           (insert "Leader: " (if leader (propertize (format "%s" leader) 'face 'zookeeper-connected)
                                (propertize "none" 'face 'zookeeper-error))
                   "\n\n")
           (zookeeper-recipe--participants
            "Participants" (plist-get res :participants)
            (lambda (it) (or (zookeeper-true-p (plist-get it :is_leader))
                             (equal (plist-get it :node) leader))))))
        ("semaphore"
         (zookeeper-recipe--participants "Leases" (plist-get res :leases) #'ignore)
         (insert "\n")
         (zookeeper-recipe--participants "Lock contenders"
                                         (plist-get res :lock_contenders) #'ignore))
        ((or "queue" "locking-queue")
         (insert (format "Size: %s   %s: %s\n\n" (plist-get res :size)
                         (if (equal kind "queue") "Ready" "Claimed")
                         (or (plist-get res :ready) (plist-get res :claimed) 0)))
         (zookeeper-recipe--queue-head (plist-get res :head)))
        ("service-registry"
         (let ((svcs (plist-get res :services)))
           (insert (propertize (format "Services (%d)\n" (length svcs)) 'face 'bold))
           (dolist (s svcs)
             (let* ((inst (plist-get s :instances))
                    (n (if (listp inst) (length inst) inst)))
               (insert "  " (propertize (format "%s" (plist-get s :name)) 'face 'bold)
                       (format "  (%s instance%s)\n" n (if (equal n 1) "" "s")))
               (when (listp inst)
                 (dolist (i inst)
                   (insert "    " (zookeeper-recipe--instance-line i) "\n")))))))
        ("service"
         (let ((inst (plist-get res :instances)))
           (insert (propertize (format "Service %s: %d instance(s)\n"
                                       (plist-get res :name) (length inst))
                               'face 'bold))
           (dolist (i inst) (insert "  " (zookeeper-recipe--instance-line i) "\n"))))
        ("counter"
         (insert (format "int64:   %s\nint32:   %s\ndecimal: %s\n"
                         (zookeeper-recipe--scalar :v (plist-get res :int64))
                         (zookeeper-recipe--scalar :v (plist-get res :int32))
                         (zookeeper-recipe--scalar :v (plist-get res :decimal)))))
        (_
         (insert (propertize "No known recipe structure detected.\n" 'face 'zookeeper-dim))
         (let ((rest (cl-loop for (k v) on res by #'cddr
                              unless (memq k '(:kind :path)) append (list k v))))
           (when rest (zookeeper-recipe--pp rest 0))))))
    (buffer-string)))

;;;###autoload
(defun zookeeper-recipe-inspect (cluster path &optional _kind)
  "Inspect the recipe structure at PATH of CLUSTER (detected automatically)."
  (interactive
   (let ((c (zookeeper-current-cluster)))
     (list c (read-string "Recipe path: " "/"))))
  (let ((res (zookeeper-recipe--call "recipe.inspect"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path))))
    (with-current-buffer (get-buffer-create (format "*zk recipe %s:%s*" cluster path))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "%s  (read-only inspection, g to refresh)\n\n" cluster)
                            'face 'zookeeper-dim))
        (insert (zookeeper-recipe-render res)))
      (special-mode)
      (setq zookeeper-cluster cluster)
      (zookeeper-setup-mode-line)
      (local-set-key "g" (lambda () (interactive) (zookeeper-recipe-inspect cluster path)))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer))
      res)))

;;;; Discovery

(defun zookeeper-discovery-browse (cluster base)
  "Browse Curator-style service discovery under BASE on CLUSTER."
  (interactive (list (zookeeper-current-cluster) (read-string "Discovery base: " "/services")))
  (let* ((id (zookeeper-cluster-id cluster))
         (services (zookeeper-recipe--call "recipe.discovery.services"
                                           (list :cluster id :base base))))
    (with-current-buffer (get-buffer-create (format "*zk discovery %s:%s*" cluster base))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Services under %s:%s (%d)\n\n" cluster base
                                    (length services))
                            'face 'bold))
        (if (null services)
            (insert (propertize "No services registered.\n" 'face 'zookeeper-dim))
          (dolist (s services)
            (let ((insts (zookeeper-recipe--call
                          "recipe.discovery.instances"
                          (list :cluster id :base base :service s))))
              (insert (propertize s 'face 'zookeeper-path)
                      (format "  (%d instance%s)\n" (length insts)
                              (if (= 1 (length insts)) "" "s")))
              (dolist (i insts)
                (insert "  " (zookeeper-recipe--instance-line i) "\n")
                (when-let* ((payload (plist-get i :payload)))
                  (insert (propertize (format "      payload: %s\n"
                                              (if (stringp payload) payload
                                                (zookeeper-json-encode payload)))
                                      'face 'zookeeper-dim))))))))
      (special-mode)
      (setq zookeeper-cluster cluster)
      (local-set-key "g" (lambda () (interactive) (zookeeper-discovery-browse cluster base)))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer))
      services)))

;;;; Counters

(defun zookeeper-counter-view (cluster path &optional encoding)
  "Show the counter at PATH on CLUSTER with ENCODING."
  (interactive (list (zookeeper-current-cluster) (read-string "Counter path: " "/")
                     (completing-read "Encoding: " '("i64" "i32" "text") nil t nil nil "i64")))
  (let ((res (zookeeper-recipe--call "recipe.counter.get"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path
                                           :encoding (or encoding "i64")))))
    (message "%s:%s = %s %s" cluster path (plist-get res :value)
             (if (equal (plist-get res :version) -1) "(node does not exist yet)"
               (format "(version %s)" (plist-get res :version))))
    res))

(defun zookeeper-counter-add (cluster path delta &optional encoding)
  "Add DELTA to the counter at PATH on CLUSTER with ENCODING."
  (interactive (list (zookeeper-current-cluster) (read-string "Counter path: " "/")
                     (read-number "Delta: " 1)))
  (zookeeper-check-writable cluster)
  (when (or (not (called-interactively-p 'interactive))
            (y-or-n-p (format "Add %s to counter %s? " delta path)))
    (let ((res (zookeeper-recipe--call "recipe.counter.add"
                                       (list :cluster (zookeeper-cluster-id cluster)
                                             :path path :delta delta
                                             :encoding (or encoding "i64")))))
      (message "%s = %s" path (plist-get res :value))
      res)))

;;;; Controlled recipes

(defvar zookeeper-recipe-confirm-function #'zookeeper-confirm-typed
  "Function called with a prompt to confirm joining a live recipe.")

(defun zookeeper-recipe--warn (what cluster path)
  "Ask for typed confirmation before starting WHAT at PATH on CLUSTER."
  (zookeeper-check-writable cluster)
  (funcall zookeeper-recipe-confirm-function
           (format "%s at %s:%s takes part in a live coordination protocol and may affect other processes."
                   what cluster path)))

(defun zookeeper-recipe--register (res kind cluster path)
  "Register recipe handle from RES of KIND on CLUSTER PATH and return its id."
  (puthash (plist-get res :recipe)
           (list :kind kind :cluster cluster :path path
                 :node (or (plist-get res :node) (plist-get res :participant))
                 :state (or (plist-get res :ownership) "held"))
           zookeeper-recipe--handles)
  (zookeeper-recipes-refresh)
  (plist-get res :recipe))

(defun zookeeper-recipe-state (recipe)
  "Return the last known ownership state of RECIPE."
  (plist-get (gethash recipe zookeeper-recipe--handles) :state))

(defun zookeeper-lock-acquire (cluster path timeout-ms)
  "Acquire a lock at PATH on CLUSTER within TIMEOUT-MS.  Return the recipe id."
  (interactive (list (zookeeper-current-cluster) (read-string "Lock path: " "/")
                     (read-number "Timeout (ms): " 5000)))
  (zookeeper-recipe--warn "Acquiring a lock" cluster path)
  (let ((res (zookeeper-recipe--call "recipe.lock.acquire"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path
                                           :timeout_ms timeout-ms))))
    (prog1 (zookeeper-recipe--register res "lock" cluster path)
      (when (called-interactively-p 'interactive) (zookeeper-recipes)))))

(defun zookeeper-latch-start (cluster path)
  "Join the leader latch at PATH on CLUSTER.  Return the recipe id."
  (interactive (list (zookeeper-current-cluster) (read-string "Latch path: " "/")))
  (zookeeper-recipe--warn "Joining a leader election" cluster path)
  (let ((res (zookeeper-recipe--call "recipe.latch.start"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path))))
    (prog1 (zookeeper-recipe--register (plist-put res :ownership "joined") "latch" cluster path)
      (when (called-interactively-p 'interactive) (zookeeper-recipes)))))

(defun zookeeper-member-join (cluster path member-id)
  "Join group membership at PATH on CLUSTER as MEMBER-ID.  Return the recipe id."
  (interactive (list (zookeeper-current-cluster) (read-string "Group path: " "/")
                     (read-string "Member id: " (system-name))))
  (zookeeper-recipe--warn "Joining a group" cluster path)
  (let ((res (zookeeper-recipe--call "recipe.member.join"
                                     (list :cluster (zookeeper-cluster-id cluster) :path path
                                           :member_id member-id))))
    (prog1 (zookeeper-recipe--register res "member" cluster path)
      (when (called-interactively-p 'interactive) (zookeeper-recipes)))))

(defun zookeeper-recipe-release (recipe)
  "Release RECIPE handle."
  (interactive
   (list (or (and (derived-mode-p 'zookeeper-recipes-mode) (tabulated-list-get-id))
             (completing-read "Release recipe: "
                              (hash-table-keys zookeeper-recipe--handles) nil t))))
  (zookeeper-recipe--call "recipe.release" (list :recipe recipe))
  ;; The "released" notification may arrive before this response; only
  ;; mark as releasing if no newer state was seen.
  (when-let* ((h (gethash recipe zookeeper-recipe--handles)))
    (when (member (plist-get h :state) '("held" "joined" "suspended"))
      (puthash recipe (plist-put h :state "releasing") zookeeper-recipe--handles)))
  (zookeeper-recipes-refresh)
  (message "Released %s" recipe))

(defun zookeeper-recipe-sync ()
  "Reload recipe handles from recipe.list."
  (interactive)
  (let ((list (zookeeper-recipe--call "recipe.list" (make-hash-table))))
    (dolist (r list)
      (let* ((id (plist-get r :recipe))
             (old (gethash id zookeeper-recipe--handles)))
        (puthash id (list :kind (plist-get r :kind)
                          :cluster (or (zookeeper--name-for-id (plist-get r :cluster))
                                       (plist-get old :cluster) (plist-get r :cluster))
                          :path (plist-get r :path)
                          :node (plist-get r :node)
                          :leader (plist-get r :leader)
                          :state (or (plist-get r :ownership) (plist-get old :state)))
                 zookeeper-recipe--handles)))
    (zookeeper-recipes-refresh)
    list))

(defun zookeeper-recipe--on-ownership (params)
  "Update recipe state from ownership notification PARAMS."
  (let* ((id (plist-get params :recipe))
         (h (or (gethash id zookeeper-recipe--handles) (list :kind "?"))))
    (setq h (plist-put h :state (plist-get params :state)))
    (when (plist-member params :leader)
      (setq h (plist-put h :leader (plist-get params :leader))))
    (puthash id h zookeeper-recipe--handles)
    (when (member (plist-get params :state) '("lost" "suspended"))
      (message "WARNING: recipe %s on %s is %s; do not assume ownership"
               id (plist-get h :path) (plist-get params :state)))
    (zookeeper-recipes-refresh)))

(add-hook 'zookeeper-recipe-ownership-functions #'zookeeper-recipe--on-ownership)

;;;; Recipes buffer

(defvar zookeeper-recipes-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m "k" #'zookeeper-recipe-release)
    (define-key m "l" #'zookeeper-lock-acquire)
    (define-key m "L" #'zookeeper-latch-start)
    (define-key m "j" #'zookeeper-member-join)
    (define-key m "g" #'zookeeper-recipe-sync)
    (define-key m "x" #'zookeeper-recipes-forget-released)
    m)
  "Keymap for `zookeeper-recipes-mode'.")

(define-derived-mode zookeeper-recipes-mode tabulated-list-mode "ZK-Recipes"
  "Recipes held by the bridge on behalf of this Emacs.
\\{zookeeper-recipes-mode-map}"
  (setq tabulated-list-format [("Id" 6 t) ("Kind" 8 t) ("Cluster" 10 t)
                               ("Ownership" 12 t) ("Leader" 6 nil) ("Path" 30 t)
                               ("Node" 40 nil)])
  (tabulated-list-init-header))

(defun zookeeper-recipe--state-face (state)
  "Face for ownership STATE."
  (pcase state
    ("held" 'zookeeper-connected)
    ((or "released" "releasing" "joined") 'zookeeper-dim)
    (_ 'zookeeper-warning-banner)))

(defun zookeeper-recipes--warning ()
  "Return a warning string when some recipe is not held, else nil."
  (let (bad)
    (maphash (lambda (id h)
               (when (member (plist-get h :state) '("lost" "suspended"))
                 (push (format "%s %s" id (plist-get h :state)) bad)))
             zookeeper-recipe--handles)
    (when bad
      (format "!! NOT HELD: %s.  Do not assume ownership. !!" (string-join bad ", ")))))

(defun zookeeper-recipes-forget-released ()
  "Remove released recipes from the list."
  (interactive)
  (maphash (lambda (id h) (when (equal (plist-get h :state) "released")
                            (remhash id zookeeper-recipe--handles)))
           zookeeper-recipe--handles)
  (zookeeper-recipes-refresh))

(defun zookeeper-recipes-refresh ()
  "Redisplay the recipes buffer if present."
  (when-let* ((b (get-buffer "*zookeeper recipes*")))
    (with-current-buffer b
      (setq tabulated-list-entries
            (cl-loop for id being the hash-keys of zookeeper-recipe--handles
                     using (hash-values h)
                     for st = (format "%s" (plist-get h :state))
                     collect (list id (vector id (format "%s" (plist-get h :kind))
                                              (format "%s" (or (plist-get h :cluster) ""))
                                              (propertize st 'face (zookeeper-recipe--state-face st))
                                              (if (zookeeper-true-p (plist-get h :leader)) "yes" "")
                                              (format "%s" (or (plist-get h :path) ""))
                                              (format "%s" (or (plist-get h :node) ""))))))
      (setq header-line-format
            (if-let* ((w (zookeeper-recipes--warning)))
                (propertize (concat " " w) 'face 'zookeeper-warning-banner)
              (propertize " Held by the bridge session.  Ownership is lost if the session is lost."
                          'face 'zookeeper-dim)))
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
