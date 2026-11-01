;;; zookeeper-node.el --- Node inspector and editor  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; One buffer per node showing stat, ACL and data.  Data can be shown
;; as text, JSON, hex or base64 and edited; C-c C-c saves with the
;; version that was read, and conflicts offer overwrite/reload/ediff.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'zookeeper-connection)

(declare-function zookeeper-acl-open "zookeeper-acl")
(declare-function ediff-buffers "ediff")
(declare-function json-pretty-print-buffer "json")

(defcustom zookeeper-node-mode-hook nil
  "Hook run when entering `zookeeper-node-mode'."
  :type 'hook
  :group 'zookeeper)

(defcustom zookeeper-node-default-view 'auto
  "Default data view: `auto', `text', `json', `hex' or `base64'."
  :type '(choice (const auto) (const text) (const json) (const hex) (const base64))
  :group 'zookeeper)

(defvar-local zookeeper-node-path nil "Path of the node in this buffer.")
(defvar-local zookeeper-node--result nil "Last node.get result plist.")
(defvar-local zookeeper-node--acl nil "Last ACL list.")
(defvar-local zookeeper-node--view 'text "Current data view.")
(defvar-local zookeeper-node--data-start nil "Marker where data begins.")
(defvar-local zookeeper-node--version nil "Data version read from the server.")
(defvar-local zookeeper-node--bytes nil "Unibyte string of the server data.")

;;;; Encoding / decoding (pure)

(defun zookeeper-data-decode (b64)
  "Decode base64 string B64 into a unibyte string."
  (if (or (null b64) (equal b64 "")) "" (base64-decode-string b64)))

(defun zookeeper-data-encode (bytes)
  "Encode unibyte BYTES as base64 without newlines."
  (base64-encode-string bytes t))

(defun zookeeper-data-utf8-p (bytes)
  "Return non-nil if unibyte BYTES are valid UTF-8."
  (let ((s (decode-coding-string bytes 'utf-8-unix t)))
    ;; Invalid sequences decode to raw-byte (eight-bit) characters.
    (not (string-match-p "[\x3fff80-\x3fffff]" s))))

(defun zookeeper-data-json-p (bytes)
  "Return non-nil if BYTES parse as JSON object or array."
  (and (zookeeper-data-utf8-p bytes)
       (string-match-p "\\`[ \t\n\r]*[[{]" bytes)
       (condition-case nil
           (and (json-parse-string (decode-coding-string bytes 'utf-8)) t)
         (error nil))))

(defun zookeeper-data-hex (bytes)
  "Return a hexdump-style string of BYTES."
  (let ((i 0) (n (length bytes)) lines)
    (while (< i n)
      (let* ((chunk (substring bytes i (min n (+ i 16)))))
        (push (format "%08x: %-48s %s" i
                      (mapconcat (lambda (b) (format "%02x" b)) chunk " ")
                      (mapconcat (lambda (b) (if (and (>= b 32) (< b 127)) (string b) "."))
                                 chunk ""))
              lines))
      (setq i (+ i 16)))
    (string-join (nreverse lines) "\n")))

(defun zookeeper-data-unhex (text)
  "Parse hexdump TEXT (as produced by `zookeeper-data-hex') to bytes."
  (apply #'unibyte-string
         (cl-loop for line in (split-string text "\n" t)
                  for body = (if (string-match "\\`[0-9a-f]+: \\(.\\{0,48\\}\\)" line)
                                 (match-string 1 line) line)
                  append (mapcar (lambda (h) (string-to-number h 16))
                                 (split-string body "[ \t]+" t)))))

(defun zookeeper-data-render (bytes view)
  "Return BYTES rendered as a multibyte string for VIEW."
  (pcase view
    ('text (decode-coding-string bytes 'utf-8-unix))
    ('json (require 'json)
           (with-temp-buffer
             (insert (decode-coding-string bytes 'utf-8-unix))
             (condition-case nil (json-pretty-print-buffer) (error nil))
             (buffer-string)))
    ('hex (zookeeper-data-hex bytes))
    ('base64 (base64-encode-string bytes))))

(defun zookeeper-data-parse (text view)
  "Convert edited TEXT in VIEW back to bytes.  Signal on invalid input."
  (pcase view
    ('text (encode-coding-string text 'utf-8-unix t))
    ('json (condition-case e
               (and (json-parse-string text)
                    (encode-coding-string text 'utf-8-unix t))
             (json-error (user-error "Invalid JSON: %s" (error-message-string e)))))
    ('hex (zookeeper-data-unhex text))
    ('base64 (condition-case nil (base64-decode-string (string-trim text))
               (error (user-error "Invalid base64"))))))

(defun zookeeper-data-auto-view (bytes)
  "Pick a view for BYTES."
  (cond ((zookeeper-data-json-p bytes) 'json)
        ((zookeeper-data-utf8-p bytes) 'text)
        (t 'hex)))

;;;; Mode

(defvar zookeeper-node-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m (kbd "C-c C-c") #'zookeeper-node-save)
    (define-key m (kbd "C-c C-k") #'zookeeper-node-revert)
    (define-key m (kbd "C-c C-v") #'zookeeper-node-cycle-view)
    (define-key m (kbd "C-c C-t") (lambda () (interactive) (zookeeper-node-set-view 'text)))
    (define-key m (kbd "C-c C-j") (lambda () (interactive) (zookeeper-node-set-view 'json)))
    (define-key m (kbd "C-c C-x") (lambda () (interactive) (zookeeper-node-set-view 'hex)))
    (define-key m (kbd "C-c C-b") (lambda () (interactive) (zookeeper-node-set-view 'base64)))
    (define-key m (kbd "C-c C-a") #'zookeeper-node-acl)
    (define-key m (kbd "C-c C-w") #'zookeeper-node-copy-path)
    m)
  "Keymap for `zookeeper-node-mode'.")

(define-derived-mode zookeeper-node-mode text-mode "ZK-Node"
  "Inspect and edit a ZooKeeper node.
The header (stat, ACL) is read-only, the data below the separator
is editable unless the cluster is read-only.
\\{zookeeper-node-mode-map}"
  (setq-local revert-buffer-function (lambda (&rest _) (zookeeper-node-revert)))
  (zookeeper-setup-mode-line))

(defun zookeeper-node-buffer-name (cluster path)
  "Return the buffer name for CLUSTER PATH."
  (format "*zk %s:%s*" cluster path))

;;;###autoload
(defun zookeeper-node-open (cluster path)
  "Open node PATH of CLUSTER in an inspector buffer."
  (interactive (let ((c (zookeeper-read-cluster)))
                 (list c (read-string "Path: " "/"))))
  (let ((buf (get-buffer-create (zookeeper-node-buffer-name cluster path))))
    (with-current-buffer buf
      (unless (derived-mode-p 'zookeeper-node-mode)
        (zookeeper-node-mode)
        (setq zookeeper-cluster cluster zookeeper-node-path path))
      (zookeeper-node-revert t))
    (pop-to-buffer buf)
    buf))

(defun zookeeper-node--fetch ()
  "Fetch data and ACL of this buffer's node."
  (setq zookeeper-node--result
        (zookeeper-call zookeeper-cluster "node.get" :path zookeeper-node-path))
  (setq zookeeper-node--acl
        (ignore-errors (plist-get (zookeeper-call zookeeper-cluster "node.acl.get"
                                                  :path zookeeper-node-path)
                                  :acl))))

(defun zookeeper-node-revert (&optional noconfirm)
  "Refetch the node, discarding edits (ask unless NOCONFIRM)."
  (interactive)
  (when (and (not noconfirm) (buffer-modified-p)
             (not (yes-or-no-p "Discard local edits? ")))
    (user-error "Cancelled"))
  (zookeeper-node--fetch)
  (setq zookeeper-node--bytes (zookeeper-data-decode (plist-get zookeeper-node--result :data)))
  (setq zookeeper-node--version (plist-get (plist-get zookeeper-node--result :stat) :version))
  (when (or noconfirm (not zookeeper-node--view))
    (setq zookeeper-node--view
          (if (eq zookeeper-node-default-view 'auto)
              (zookeeper-data-auto-view zookeeper-node--bytes)
            zookeeper-node-default-view)))
  (zookeeper-node--render))

(defun zookeeper-node-format-stat (stat)
  "Return a human-readable string for STAT."
  (let ((owner (zookeeper-format-session (plist-get stat :ephemeral_owner))))
    (concat
     (format "  created   %s  (czxid 0x%x)\n" (zookeeper-format-time (plist-get stat :ctime))
             (or (plist-get stat :czxid) 0))
     (format "  modified  %s  (mzxid 0x%x)\n" (zookeeper-format-time (plist-get stat :mtime))
             (or (plist-get stat :mzxid) 0))
     (format "  version %s  cversion %s  aversion %s\n"
             (plist-get stat :version) (plist-get stat :cversion) (plist-get stat :aversion))
     (format "  size %s  children %s%s\n"
             (zookeeper-format-size (plist-get stat :data_length))
             (plist-get stat :num_children)
             (if owner (propertize (format "  EPHEMERAL owner %s" owner)
                                   'face 'zookeeper-ephemeral)
               "")))))

(defun zookeeper-node--render ()
  "Render the node buffer."
  (let* ((inhibit-read-only t)
         (res zookeeper-node--result)
         (stat (plist-get res :stat))
         (ro (zookeeper-profile-read-only-p zookeeper-cluster)))
    (erase-buffer)
    (insert (propertize (format "%s:%s" zookeeper-cluster zookeeper-node-path)
                        'face 'zookeeper-path)
            (if ro (propertize "  [read-only]" 'face 'zookeeper-dim) "")
            "\n")
    (insert (zookeeper-node-format-stat stat))
    (insert "  acl     "
            (if zookeeper-node--acl
                (mapconcat (lambda (a) (format "%s:%s:%s" (plist-get a :scheme)
                                               (plist-get a :id) (plist-get a :perms)))
                           zookeeper-node--acl ", ")
              "?")
            "\n")
    (when (zookeeper-true-p (plist-get res :truncated))
      (insert (propertize "  DATA TRUNCATED: saving disabled\n" 'face 'zookeeper-error)))
    (insert (propertize (format "-- data (%s view; C-c C-v cycle, C-c C-c save) --\n"
                                zookeeper-node--view)
                        'face 'zookeeper-dim))
    (add-text-properties (point-min) (point)
                         '(read-only t front-sticky t rear-nonsticky t))
    (setq zookeeper-node--data-start (point-marker))
    (insert (zookeeper-data-render zookeeper-node--bytes zookeeper-node--view))
    (goto-char zookeeper-node--data-start)
    (setq buffer-read-only ro)
    (set-buffer-modified-p nil)))

(defun zookeeper-node--data-text ()
  "Return the edited data text."
  (buffer-substring-no-properties zookeeper-node--data-start (point-max)))

(defconst zookeeper-node-views '(text json hex base64) "Available data views.")

(defun zookeeper-node-set-view (view)
  "Switch the data display to VIEW, keeping edits."
  (let ((bytes (zookeeper-data-parse (zookeeper-node--data-text) zookeeper-node--view))
        (modified (buffer-modified-p))
        (saved zookeeper-node--bytes))
    (setq zookeeper-node--view view)
    (setq zookeeper-node--bytes bytes)
    (let ((buffer-read-only nil)) (zookeeper-node--render))
    (setq zookeeper-node--bytes saved)
    (set-buffer-modified-p modified)))

(defun zookeeper-node-cycle-view ()
  "Cycle between text, JSON, hex and base64 views."
  (interactive)
  (zookeeper-node-set-view
   (or (cadr (memq zookeeper-node--view zookeeper-node-views)) 'text)))

(defun zookeeper-node-save (&optional force-version)
  "Save the data with node.set using the version read.
With FORCE-VERSION, use that version instead."
  (interactive)
  (zookeeper-check-writable zookeeper-cluster)
  (when (zookeeper-true-p (plist-get zookeeper-node--result :truncated))
    (user-error "Data was truncated; refusing to save"))
  (let* ((bytes (zookeeper-data-parse (zookeeper-node--data-text) zookeeper-node--view))
         (version (or force-version zookeeper-node--version)))
    (condition-case err
        (let ((res (zookeeper--with-force-node
                    (lambda (force)
                      (apply #'zookeeper-call zookeeper-cluster "node.set"
                             :path zookeeper-node-path
                             :data (zookeeper-data-encode bytes)
                             :version version
                             (and force (list :force t)))))))
          (setq zookeeper-node--version (plist-get (plist-get res :stat) :version))
          (setq zookeeper-node--bytes bytes)
          (setf (plist-get zookeeper-node--result :stat) (plist-get res :stat))
          (let ((pos (point)))
            (zookeeper-node--render)
            (goto-char (min pos (point-max))))
          (message "Saved %s (version %s)" zookeeper-node-path zookeeper-node--version))
      (zookeeper-error
       (pcase (zookeeper-error-kind err)
         ("bad-version" (zookeeper-node--conflict bytes))
         ("outcome-unknown"
          (message "Outcome unknown: re-read before retrying (C-c C-k)"))
         (_ (signal (car err) (cdr err))))))))

(defun zookeeper--with-force-node (fn)
  "Call FN with nil, offering a forced retry on protected paths."
  (condition-case err (funcall fn nil)
    (zookeeper-error
     (if (and (equal (zookeeper-error-kind err) "policy-denied")
              (not (string-match-p "read.only" (or (zookeeper-error-message err) "")))
              (zookeeper-confirm-force "this node"))
         (funcall fn t)
       (signal (car err) (cdr err))))))

(defun zookeeper-node--conflict (local-bytes)
  "Handle a version conflict while saving LOCAL-BYTES."
  (let* ((server (zookeeper-call zookeeper-cluster "node.get" :path zookeeper-node-path))
         (sv (plist-get (plist-get server :stat) :version))
         (sbytes (zookeeper-data-decode (plist-get server :data)))
         (choice (read-multiple-choice
                  (format "Conflict: node changed on server (read v%s, now v%s)"
                          zookeeper-node--version sv)
                  '((?o "overwrite" "Write local data over the new server version")
                    (?r "reload" "Discard local edits and reload")
                    (?e "ediff" "Compare local and server versions")
                    (?q "quit" "Keep editing")))))
    (pcase (car choice)
      (?o (when (yes-or-no-p (format "Overwrite server version %s? " sv))
            (zookeeper-node-save sv)))
      (?r (zookeeper-node-revert t))
      (?e (zookeeper-node--ediff local-bytes sbytes))
      (_ (message "Still editing; server is at version %s" sv)))))

(defun zookeeper-node--ediff (local server)
  "Run ediff between LOCAL and SERVER byte strings."
  (let ((view zookeeper-node--view)
        (name zookeeper-node-path))
    (let ((a (get-buffer-create (format "*zk local %s*" name)))
          (b (get-buffer-create (format "*zk server %s*" name))))
      (dolist (pair (list (cons a local) (cons b server)))
        (with-current-buffer (car pair)
          (let ((inhibit-read-only t))
            (erase-buffer)
            (insert (zookeeper-data-render (cdr pair) view)))))
      (ediff-buffers a b))))

(defun zookeeper-node-acl ()
  "Open the ACL editor for this node."
  (interactive)
  (require 'zookeeper-acl)
  (zookeeper-acl-open zookeeper-cluster zookeeper-node-path))

(defun zookeeper-node-copy-path ()
  "Copy the node path."
  (interactive)
  (kill-new zookeeper-node-path)
  (message "%s" zookeeper-node-path))

(provide 'zookeeper-node)
;;; zookeeper-node.el ends here
