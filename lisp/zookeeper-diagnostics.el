;;; zookeeper-diagnostics.el --- Health, status and audit views  -*- lexical-binding: t; -*-

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Cluster health (cluster.health), client status (cluster.status),
;; four-letter-word commands, the audit log (audit.list) and the
;; bridge log.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'tabulated-list)
(require 'zookeeper-connection)

(declare-function zookeeper-recipe--pp "zookeeper-recipe")

(defun zookeeper-diagnostics--show (name title fn)
  "Display buffer NAME with TITLE, filled by calling FN."
  (with-current-buffer (get-buffer-create name)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (propertize title 'face 'bold) "\n\n")
      (funcall fn))
    (special-mode)
    (goto-char (point-min))
    (pop-to-buffer (current-buffer))))

;;;###autoload
(defun zookeeper-cluster-health (cluster)
  "Show the health of each server of CLUSTER."
  (interactive (list (zookeeper-current-cluster)))
  (let ((res (zookeeper-call cluster "cluster.health")))
    (zookeeper-diagnostics--show
     (format "*zk health %s*" cluster) (format "Health of %s" cluster)
     (lambda ()
       (dolist (h res)
         (insert (propertize (if (zookeeper-true-p (plist-get h :ok)) "OK  " "FAIL")
                             'face (if (zookeeper-true-p (plist-get h :ok))
                                       'zookeeper-connected 'zookeeper-error))
                 "  " (format "%s" (plist-get h :host)) "\n")
         (when-let* ((e (plist-get h :error)))
           (insert "      " (propertize (format "%s" (if (listp e) (plist-get e :message) e))
                                        'face 'zookeeper-error)
                   "\n"))
         (when-let* ((info (plist-get h :info)))
           (require 'zookeeper-recipe)
           (zookeeper-recipe--pp info 6)))))))

;;;###autoload
(defun zookeeper-cluster-status (cluster)
  "Show client diagnostics for CLUSTER."
  (interactive (list (zookeeper-current-cluster)))
  (let ((res (zookeeper-call cluster "cluster.status")))
    (require 'zookeeper-recipe)
    (zookeeper-diagnostics--show
     (format "*zk status %s*" cluster) (format "Client status of %s" cluster)
     (lambda () (zookeeper-recipe--pp res 0)))))

(defconst zookeeper-four-letter-words
  '("ruok" "srvr" "stat" "conf" "envi" "mntr" "cons" "wchs" "isro" "dirs")
  "Commonly allowed four-letter commands.")

(defun zookeeper-four-letter (cluster command &optional host)
  "Run four-letter COMMAND on CLUSTER (optionally on HOST)."
  (interactive (list (zookeeper-current-cluster)
                     (completing-read "Command: " zookeeper-four-letter-words nil nil nil nil "srvr")
                     (let ((h (read-string "Host (empty = any): ")))
                       (unless (string-empty-p h) h))))
  (let ((res (apply #'zookeeper-call cluster "cluster.four-letter" :command command
                    (and host (list :host host)))))
    (zookeeper-diagnostics--show
     (format "*zk %s %s*" command cluster)
     (format "%s on %s" command (plist-get res :host))
     (lambda () (insert (or (plist-get res :output) ""))))))

(define-derived-mode zookeeper-audit-mode tabulated-list-mode "ZK-Audit"
  "Audit records of writes made through this bridge."
  (setq tabulated-list-format [("Time" 19 t) ("Cluster" 10 t) ("Operation" 14 t)
                               ("Outcome" 10 t) ("Forced" 6 nil) ("Paths" 50 nil)])
  (tabulated-list-init-header))

;;;###autoload
(defun zookeeper-audit-log (&optional limit)
  "Show the last LIMIT audit records from the bridge."
  (interactive "P")
  (let ((recs (zookeeper-rpc "audit.list" (list :limit (if limit (prefix-numeric-value limit) 200)))))
    (when (and (listp recs) (keywordp (car-safe recs))) (setq recs (plist-get recs :records)))
    (with-current-buffer (get-buffer-create "*zookeeper audit*")
      (zookeeper-audit-mode)
      (setq tabulated-list-entries
            (cl-loop for r in recs for i from 0
                     collect (list i (vector (zookeeper-format-time (plist-get r :at_ms))
                                             (format "%s" (plist-get r :cluster))
                                             (format "%s" (plist-get r :operation))
                                             (format "%s" (plist-get r :outcome))
                                             (if (zookeeper-true-p (plist-get r :forced)) "yes" "")
                                             (mapconcat (lambda (p) (format "%s" p))
                                                        (plist-get r :paths) " ")))))
      (tabulated-list-print)
      (pop-to-buffer (current-buffer)))))

(provide 'zookeeper-diagnostics)
;;; zookeeper-diagnostics.el ends here
