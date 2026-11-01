;;; zookeeper-acl.el --- ACL viewer and editor  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Tabulated ACL editor.  Edits are local until saved with C-c C-c,
;; which uses node.acl.set with the aversion that was read.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'tabulated-list)
(require 'zookeeper-connection)

(defconst zookeeper-acl-perm-bits '((?r . 1) (?w . 2) (?c . 4) (?d . 8) (?a . 16))
  "ZooKeeper permission letters and bits.")

(defun zookeeper-acl-perms-to-int (perms)
  "Convert PERMS (string like \"cdrwa\" or integer) to a bit mask."
  (if (integerp perms) perms
    (cl-loop for ch across (downcase perms)
             for bit = (alist-get ch zookeeper-acl-perm-bits)
             unless bit do (user-error "Invalid permission %c" ch)
             sum bit)))

(defun zookeeper-acl-perms-to-string (perms)
  "Convert PERMS (integer or string) to canonical \"cdrwa\" order."
  (let ((n (zookeeper-acl-perms-to-int perms)))
    (apply #'string (cl-loop for ch across "cdrwa"
                             when (/= 0 (logand n (alist-get ch zookeeper-acl-perm-bits)))
                             collect ch))))

(defun zookeeper-acl-toggle-perm (perms ch)
  "Return PERMS string with permission CH toggled."
  (zookeeper-acl-perms-to-string
   (logxor (zookeeper-acl-perms-to-int perms) (alist-get ch zookeeper-acl-perm-bits))))

(defun zookeeper-acl-parse-entry (string)
  "Parse \"scheme:id:perms\" STRING into an ACL plist.
The id may contain colons (digest ids do)."
  (if (string-match "\\`\\([^:]+\\):\\(.*\\):\\([cdrwa]*\\)\\'" string)
      (list :scheme (match-string 1 string) :id (match-string 2 string)
            :perms (zookeeper-acl-perms-to-string (match-string 3 string)))
    (user-error "ACL must be scheme:id:perms")))

(defun zookeeper-acl-format-entry (entry)
  "Format ACL ENTRY plist as scheme:id:perms."
  (format "%s:%s:%s" (plist-get entry :scheme) (plist-get entry :id)
          (zookeeper-acl-perms-to-string (plist-get entry :perms))))

(defvar-local zookeeper-acl-path nil "Node path of this ACL buffer.")
(defvar-local zookeeper-acl--entries nil "Current (possibly edited) entries.")
(defvar-local zookeeper-acl--aversion nil "ACL version read.")
(defvar-local zookeeper-acl--modified nil "Non-nil when edited.")

(defvar zookeeper-acl-mode-map
  (let ((m (make-sparse-keymap)))
    (define-key m "+" #'zookeeper-acl-add)
    (define-key m "i" #'zookeeper-acl-add)
    (define-key m "k" #'zookeeper-acl-remove)
    (define-key m "e" #'zookeeper-acl-edit)
    (define-key m "D" #'zookeeper-acl-add-digest)
    (dolist (ch '(?c ?d ?r ?w ?a))
      (define-key m (string ch) (lambda () (interactive) (zookeeper-acl-toggle ch))))
    (define-key m (kbd "C-c C-c") #'zookeeper-acl-save)
    (define-key m "g" #'zookeeper-acl-reload)
    m)
  "Keymap for `zookeeper-acl-mode'.")

(define-derived-mode zookeeper-acl-mode tabulated-list-mode "ZK-ACL"
  "Edit a node ACL.  Toggle perms with c/d/r/w/a, save with \\[zookeeper-acl-save].
\\{zookeeper-acl-mode-map}"
  (setq tabulated-list-format [("Scheme" 10 t) ("Id" 50 t) ("Perms" 6 nil)])
  (setq tabulated-list-padding 2)
  (zookeeper-setup-mode-line)
  (tabulated-list-init-header))

;;;###autoload
(defun zookeeper-acl-open (cluster path)
  "Show the ACL of PATH on CLUSTER."
  (interactive (let ((c (zookeeper-read-cluster))) (list c (read-string "Path: " "/"))))
  (with-current-buffer (get-buffer-create (format "*zk acl %s:%s*" cluster path))
    (zookeeper-acl-mode)
    (setq zookeeper-cluster cluster zookeeper-acl-path path)
    (zookeeper-acl-reload)
    (pop-to-buffer (current-buffer))))

(defun zookeeper-acl-reload ()
  "Reload the ACL from the server."
  (interactive)
  (let ((res (zookeeper-call zookeeper-cluster "node.acl.get" :path zookeeper-acl-path)))
    (setq zookeeper-acl--entries (plist-get res :acl)
          zookeeper-acl--aversion (plist-get (plist-get res :stat) :aversion)
          zookeeper-acl--modified nil)
    (zookeeper-acl--print)))

(defun zookeeper-acl--print ()
  "Redisplay entries."
  (setq tabulated-list-entries
        (cl-loop for e in zookeeper-acl--entries for i from 0
                 collect (list i (vector (plist-get e :scheme) (plist-get e :id)
                                         (zookeeper-acl-perms-to-string (plist-get e :perms))))))
  (setq header-line-format
        (format " %s  aversion %s%s" zookeeper-acl-path zookeeper-acl--aversion
                (if zookeeper-acl--modified "  [modified, C-c C-c to save]" "")))
  (tabulated-list-print t))

(defun zookeeper-acl--index ()
  "Return the entry index at point."
  (or (tabulated-list-get-id) (user-error "No entry here")))

(defun zookeeper-acl--changed ()
  "Mark modified and redisplay."
  (setq zookeeper-acl--modified t)
  (zookeeper-acl--print))

(defun zookeeper-acl-toggle (ch)
  "Toggle permission CH on the entry at point."
  (let* ((i (zookeeper-acl--index)) (e (copy-sequence (nth i zookeeper-acl--entries))))
    (setf (nth i zookeeper-acl--entries)
          (plist-put e :perms (zookeeper-acl-toggle-perm (plist-get e :perms) ch)))
    (zookeeper-acl--changed)))

(defun zookeeper-acl-add (spec)
  "Add an entry from SPEC (scheme:id:perms)."
  (interactive "sNew ACL entry (scheme:id:perms): ")
  (setq zookeeper-acl--entries (append zookeeper-acl--entries
                                       (list (zookeeper-acl-parse-entry spec))))
  (zookeeper-acl--changed))

(defun zookeeper-acl-edit ()
  "Edit the entry at point."
  (interactive)
  (let ((i (zookeeper-acl--index)))
    (setf (nth i zookeeper-acl--entries)
          (zookeeper-acl-parse-entry
           (read-string "ACL: " (zookeeper-acl-format-entry (nth i zookeeper-acl--entries)))))
    (zookeeper-acl--changed)))

(defun zookeeper-acl-remove ()
  "Remove the entry at point."
  (interactive)
  (let ((i (zookeeper-acl--index)))
    (setq zookeeper-acl--entries
          (append (seq-take zookeeper-acl--entries i) (nthcdr (1+ i) zookeeper-acl--entries)))
    (zookeeper-acl--changed)))

(defun zookeeper-acl-digest (user password)
  "Return the digest id for USER and PASSWORD via acl.digest."
  (plist-get (zookeeper-rpc "acl.digest" (list :user user :password password)) :id))

(defun zookeeper-acl-add-digest (user perms)
  "Add a digest entry for USER with PERMS; the password is read securely."
  (interactive "sDigest user: \nsPerms (cdrwa): ")
  (let* ((pw (read-passwd "Password: "))
         (id (prog1 (zookeeper-acl-digest user pw) (clear-string pw))))
    (setq zookeeper-acl--entries
          (append zookeeper-acl--entries
                  (list (list :scheme "digest" :id id
                              :perms (zookeeper-acl-perms-to-string perms)))))
    (zookeeper-acl--changed)))

(defun zookeeper-acl-save ()
  "Save the ACL with an aversion check."
  (interactive)
  (zookeeper-check-writable zookeeper-cluster)
  (unless zookeeper-acl--entries (user-error "Refusing to set an empty ACL"))
  (unless (cl-some (lambda (e) (string-match-p "a" (zookeeper-acl-perms-to-string
                                                   (plist-get e :perms))))
                   zookeeper-acl--entries)
    (unless (yes-or-no-p "No entry keeps ADMIN permission; you may lock yourself out.  Continue? ")
      (user-error "Cancelled")))
  (when (y-or-n-p (format "Set ACL of %s? " zookeeper-acl-path))
    (condition-case err
        (zookeeper-call zookeeper-cluster "node.acl.set" :path zookeeper-acl-path
                        :acl (vconcat (mapcar (lambda (e)
                                                (list :scheme (plist-get e :scheme)
                                                      :id (plist-get e :id)
                                                      :perms (zookeeper-acl-perms-to-string
                                                              (plist-get e :perms))))
                                              zookeeper-acl--entries))
                        :version zookeeper-acl--aversion)
      (zookeeper-error
       (if (equal (zookeeper-error-kind err) "bad-version")
           (user-error "ACL changed on the server; press g to reload")
         (signal (car err) (cdr err)))))
    (zookeeper-acl-reload)
    (message "ACL saved")))

(provide 'zookeeper-acl)
;;; zookeeper-acl.el ends here
