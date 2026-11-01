;;; zookeeper.el --- ZooKeeper browser and toolkit  -*- lexical-binding: t; -*-

;; Author: emacs-zookeeper contributors
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1") (transient "0.4"))
;; Keywords: tools, data
;; URL: https://github.com/emacs-zookeeper/emacs-zookeeper

;; This file is not part of GNU Emacs.

;;; Commentary:

;; Emacs front-end for Apache ZooKeeper.  All ZooKeeper traffic goes
;; through the `zk-bridge' helper process (JSON-RPC over stdio).
;;
;; Configure `zookeeper-clusters', then run M-x zookeeper.
;;
;;   (setq zookeeper-clusters
;;         '(("local" :connect "127.0.0.1:2181")
;;           ("prod" :connect "zk1:2181,zk2:2181" :read-only t
;;            :protected-paths ("/kafka/**")
;;            :auth ((:scheme "digest"
;;                    :credential (:command ("pass" "show" "zk/prod")))))))

;;; Code:

(require 'transient)
(require 'zookeeper-bridge)
(require 'zookeeper-connection)
(require 'zookeeper-browser)
(require 'zookeeper-node)
(require 'zookeeper-acl)
(require 'zookeeper-watch)
(require 'zookeeper-recipe)
(require 'zookeeper-diagnostics)

;;;###autoload
(defun zookeeper (cluster)
  "Pick CLUSTER and open its tree browser."
  (interactive (list (zookeeper-read-cluster "ZooKeeper cluster: ")))
  (zookeeper-browse cluster))

;;;###autoload (autoload 'zookeeper-dispatch "zookeeper" nil t)
(transient-define-prefix zookeeper-dispatch ()
  "Main ZooKeeper menu."
  [["Clusters"
    ("o" "Open browser" zookeeper)
    ("c" "Connect" zookeeper-connect)
    ("k" "Disconnect" zookeeper-disconnect)
    ("l" "Connections" zookeeper-list-connections)]
   ["Nodes"
    ("j" "Go to path" zookeeper-goto-path)
    ("v" "Open node" zookeeper-node-open)
    ("r" "Recent node" zookeeper-visit-recent)
    ("s" "Search" zookeeper-search)
    ("=" "Diff subtrees" zookeeper-diff)
    ("a" "ACL" zookeeper-acl-open)]
   ["Watches"
    ("e" "Events log" zookeeper-events)
    ("w" "Add watch" zookeeper-watch-add)
    ("W" "List watches" zookeeper-watch-list)
    ("-" "Remove watch" zookeeper-watch-remove)]
   ["Recipes"
    ("i" "Inspect recipe" zookeeper-recipe-inspect)
    ("S" "Service discovery" zookeeper-discovery-browse)
    ("n" "Counter" zookeeper-counter-view)
    ("R" "Held recipes" zookeeper-recipes)]
   ["Diagnostics"
    ("h" "Health" zookeeper-cluster-health)
    ("t" "Status" zookeeper-cluster-status)
    ("4" "Four-letter word" zookeeper-four-letter)
    ("A" "Audit log" zookeeper-audit-log)
    ("L" "Bridge log" zookeeper-bridge-show-log)
    ("B" "Restart bridge" zookeeper-bridge-restart)]])

(provide 'zookeeper)
;;; zookeeper.el ends here
