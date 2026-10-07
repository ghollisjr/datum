;;; test-admin-display.el --- Batch tests for admin panel display  -*- lexical-binding: t; -*-

;; Covers when an admin panel buffer is raised into a window.  The rule
;; has two halves that pull against each other:
;;
;;   - an auto-refresh must never steal focus or drag a buried panel back
;;   - an explicit request (C-c s a, or B to go back) must always show it
;;
;; Panel data arrives asynchronously, so the display code cannot tell the
;; two apart on its own; `sql-datum--admin-display-request' carries the
;; distinction.  Before it existed, re-running C-c s a on an already-open
;; panel silently updated the buffer without showing it.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-admin-display.el

;;; Code:

(require 'cl-lib)

(defvar test-admin-display--pass 0)
(defvar test-admin-display--fail 0)

(defmacro test-admin-display-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-admin-display--pass (1+ test-admin-display--pass))
             (message "  PASS: %s" ,description))
         (setq test-admin-display--fail (1+ test-admin-display--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-admin-display--fail (1+ test-admin-display--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(defun test-admin-display--panel (panel)
  "Return parsed panel data for PANEL, as the envelope handler would."
  (sql-datum--admin-denull-alist
   (json-parse-string
    (format (concat "{\"panel\":\"%s\",\"headers\":[\"A\",\"B\"],"
                    "\"rows\":[[\"1\",\"x\"],[\"2\",\"y\"]],"
                    "\"row_id\":0,\"actions\":[],\"info\":null}")
            panel)
    :object-type 'alist :array-type 'list)))

(defun test-admin-display--visible-p (name)
  "Return non-nil if buffer NAME is displayed in some window."
  (let ((buf (get-buffer name)))
    (and buf (get-buffer-window buf t) t)))

;; A request belongs to the connection that made it, so the tests need
;; one to make it on.
(defvar test-admin-display--connection
  (get-buffer-create "*SQL: display-test*"))

(defun test-admin-display--show (panel &optional requested)
  "Deliver panel data for PANEL, as an explicit REQUESTED one if non-nil."
  (when requested
    (sql-datum--admin-request-display panel
                                      test-admin-display--connection))
  (sql-datum--admin-show-panel (test-admin-display--panel panel)
                               test-admin-display--connection))

(message "\n=== Admin panel display ===")

(let ((db "*datum-admin:databases [display-test]*")
      (jobs "*datum-admin:jobs [display-test]*"))
  ;; Start from a clean slate so a stale buffer cannot mask a failure.
  (dolist (name (list db jobs))
    (when (get-buffer name) (kill-buffer name)))
  (with-current-buffer test-admin-display--connection
    (setq sql-datum--admin-display-request nil))

  (test-admin-display--show "databases" t)
  (test-admin-display-assert "first request displays the panel"
                             (test-admin-display--visible-p db))
  (test-admin-display-assert "request flag is consumed"
                             (null (buffer-local-value
                                    'sql-datum--admin-display-request
                                    test-admin-display--connection)))

  (test-admin-display--show "databases")
  (test-admin-display-assert "refresh leaves a visible panel visible"
                             (test-admin-display--visible-p db))

  (delete-windows-on (get-buffer db))
  (test-admin-display-assert "panel can be buried"
                             (not (test-admin-display--visible-p db)))
  (test-admin-display--show "databases")
  (test-admin-display-assert "refresh does NOT resurrect a buried panel"
                             (not (test-admin-display--visible-p db)))

  (test-admin-display--show "databases" t)
  (test-admin-display-assert "explicit request re-displays a buried panel"
                             (test-admin-display--visible-p db))

  ;; A pending request must only be satisfied by its own panel.
  (delete-windows-on (get-buffer db))
  (setq sql-datum--admin-display-request "jobs")
  (test-admin-display--show "databases")
  (test-admin-display-assert "another panel's refresh does not consume the flag"
                             (and (not (test-admin-display--visible-p db))
                                  (equal sql-datum--admin-display-request
                                         "jobs")))
  (test-admin-display--show "jobs")
  (test-admin-display-assert "the requested panel is displayed when it arrives"
                             (test-admin-display--visible-p jobs)))


(message "\n=== a request belongs to the connection that made it ===")

;; Held in one place for every connection, a request made on one would be
;; answered by whichever replied first: the wrong panel raised, and the
;; asked-for one left buried.
(let ((prod (get-buffer-create "*SQL: prod*"))
      (dev (get-buffer-create "*SQL: dev*")))
  (cl-flet ((deliver (sqli panel)
              (sql-datum--admin-show-panel
               (test-admin-display--panel panel) sqli)))
    (deliver prod "databases")
    (deliver dev "databases")
    (let ((prod-panel "*datum-admin:databases [prod]*")
          (dev-panel "*datum-admin:databases [dev]*"))
      (test-admin-display-assert "each connection has its own panel"
                                 (and (get-buffer prod-panel)
                                      (get-buffer dev-panel)))
      ;; Bury both, so only an explicit request can raise one.
      (dolist (name (list prod-panel dev-panel))
        (delete-windows-on (get-buffer name)))
      (test-admin-display-assert "both are buried to begin with"
                                 (and (not (test-admin-display--visible-p
                                            prod-panel))
                                      (not (test-admin-display--visible-p
                                            dev-panel))))
      ;; prod asks; dev answers first.
      (sql-datum--admin-request-display "databases" prod)
      (deliver dev "databases")
      (test-admin-display-assert
       "another connection's refresh does not take the request"
       (not (test-admin-display--visible-p dev-panel)))
      (test-admin-display-assert
       "and leaves the asked-for panel still to come"
       ;; A request names the sub-panel too, so that a list refreshing
       ;; on its timer cannot answer a request made for a sub-panel.
       (equal (buffer-local-value 'sql-datum--admin-display-request prod)
              '("databases")))
      (deliver prod "databases")
      (test-admin-display-assert "which surfaces when its own data arrives"
                                 (test-admin-display--visible-p prod-panel))
      (test-admin-display-assert "the request having been used up"
                                 (null (buffer-local-value
                                        'sql-datum--admin-display-request
                                        prod))))))

(message "\n=== a panel names its own connection ===")

;; A panel that can drop a database must not put another machine's name
;; over its rows.  Once prod's connection was closed, its still-open
;; panel read "on dev-sql-09" -- the other connection, found by the same
;; fallback that sending a command uses.

(defun test-admin-display--connect (name server)
  "Return a stand-in SQLi buffer called NAME, connected to SERVER."
  (let ((buf (get-buffer-create (format "*SQL: %s*" name))))
    (with-current-buffer buf
      (setq-local sql-datum--meta (make-hash-table :test #'equal))
      (puthash "server" server sql-datum--meta))
    buf))

(defun test-admin-display--jobs-on (conn)
  "Render the jobs panel as CONN would answer it, and return its buffer."
  (with-current-buffer conn
    (sql-datum--handle-admin-panel
     (concat "{\"panel\":\"jobs\",\"headers\":[\"Job Name\"],"
             "\"rows\":[[\"nightly load\"]],\"row_id\":0,"
             "\"actions\":[],\"info\":null}")))
  (sit-for 0.1)
  (get-buffer (sql-datum--admin-buffer-name "jobs" nil conn)))

(defun test-admin-display--header (buf)
  "Return the line under the title of panel BUF."
  (with-current-buffer buf
    (save-excursion
      (goto-char (point-min))
      (forward-line 1)
      (buffer-substring-no-properties (point) (line-end-position)))))

(let* ((prod (test-admin-display--connect "prod" "prod-sql-01"))
       (dev  (test-admin-display--connect "dev" "dev-sql-09"))
       (prod-panel (test-admin-display--jobs-on prod))
       (dev-panel  (test-admin-display--jobs-on dev)))
  ;; What a live Emacs answers when asked for "a datum connection": one
  ;; of them, not necessarily the one meant.
  (cl-letf (((symbol-function 'sql-find-sqli-buffer) (lambda (&rest _) dev)))
    (test-admin-display-assert
     "each panel names the connection it came from"
     (and (string-match-p "on prod-sql-01"
                          (test-admin-display--header prod-panel))
          (string-match-p "on dev-sql-09"
                          (test-admin-display--header dev-panel))))

    (kill-buffer prod)
    (with-current-buffer prod-panel (sql-datum-admin-sort-by-column 0))

    (test-admin-display-assert
     "a panel whose connection closed keeps naming its own server"
     (string-match-p "on prod-sql-01"
                     (test-admin-display--header prod-panel)))
    (test-admin-display-assert
     "and never borrows the name of one that is still open"
     (not (string-match-p "dev-sql-09"
                          (test-admin-display--header prod-panel))))
    (test-admin-display-assert
     "saying plainly that it is no longer connected"
     (string-match-p "disconnected" (test-admin-display--header prod-panel)))

    ;; The tag went with the connection too, so the re-render landed in a
    ;; second, untagged panel while the one on screen stayed as it was.
    (test-admin-display-assert
     "re-rendering stays in the panel it is already in"
     (null (get-buffer "*datum-admin:jobs*")))
    (test-admin-display-assert
     "and leaves the other connection's panel alone"
     (string-match-p "on dev-sql-09"
                     (test-admin-display--header dev-panel)))

    ;; Polling on a closed connection falls back the same way, so a panel
    ;; of prod's jobs would go on asking dev for its jobs every few
    ;; seconds.
    (let (sent)
      (cl-letf (((symbol-function 'sql-datum--enqueue-one)
                 (lambda (cmd &rest _) (push cmd sent)))
                ((symbol-function 'get-buffer-process) (lambda (b) (and b t))))
        (sql-datum--admin-tick prod-panel)
        (test-admin-display-assert
         "a disconnected panel asks nobody for a refresh"
         (null sent))
        (test-admin-display-assert
         "and stops its timer"
         (null (buffer-local-value 'sql-datum--admin-timer prod-panel)))
        (test-admin-display-assert
         "saying so where it claimed to be refreshing"
         (string-match-p "auto-refresh off"
                         (test-admin-display--header prod-panel)))
        (setq sent nil)
        (sql-datum--admin-tick dev-panel)
        (test-admin-display-assert
         "while a live panel goes on refreshing"
         (equal sent '(":admin jobs"))))))
  (dolist (b (list dev prod-panel dev-panel))
    (when (buffer-live-p b) (kill-buffer b))))

(message "\n=== a detail view is somewhere to go and work ===")

;; `display-buffer' showed a job's tree without selecting it, so the
;; cursor stayed behind in the list and every key pressed next went to
;; the wrong buffer.  It is selected now, under the same rule the
;; tabular panels follow.

(defun test-admin-display--tree ()
  "Parsed panel data for a job's tree."
  (sql-datum--admin-denull-alist
   (json-parse-string
    (concat "{\"panel\":\"jobs\",\"sub_panel\":\"detail\","
            "\"title\":\"Job: nightly load\","
            "\"sections\":[{\"title\":\"Steps\",\"headers\":[\"Step\"],"
            "\"rows\":[[\"1\"]],\"row_id\":0,\"actions\":[]}],"
            "\"info\":null,\"context\":{\"job_name\":\"nightly load\"}}")
    :object-type 'alist :array-type 'list)))

(defun test-admin-display--selected ()
  "Return the name of the buffer the cursor is actually in."
  (buffer-name (window-buffer (selected-window))))

(let ((conn test-admin-display--connection)
      (list-buf "*datum-admin:jobs [display-test]*")
      (tree-buf "*datum-admin:jobs:detail [display-test]*"))
  (dolist (name (list list-buf tree-buf))
    (when (get-buffer name) (kill-buffer name)))
  (with-current-buffer conn (setq sql-datum--admin-display-request nil))

  (test-admin-display--show "jobs" t)
  (switch-to-buffer list-buf)

  ;; d on a job.
  (sql-datum--admin-request-display "jobs" conn "detail")
  (sql-datum--admin-show-detail (test-admin-display--tree) conn)
  (test-admin-display-assert "opening a job's tree puts the cursor in it"
                             (equal (test-admin-display--selected) tree-buf))

  ;; A refresh nobody asked for must not drag the cursor away.
  (switch-to-buffer list-buf)
  (sql-datum--admin-show-detail (test-admin-display--tree) conn)
  (test-admin-display-assert "a refresh of the tree leaves the cursor alone"
                             (equal (test-admin-display--selected) list-buf))

  ;; Asking again for a tree already built must show it again -- under
  ;; the old rule it was only ever shown the first time.
  (sql-datum--admin-request-display "jobs" conn "detail")
  (sql-datum--admin-show-detail (test-admin-display--tree) conn)
  (test-admin-display-assert "asking for it again shows it again"
                             (equal (test-admin-display--selected) tree-buf))

  ;; The list refreshes every few seconds; that must not answer for the
  ;; tree and leave the thing actually asked for unshown.
  (switch-to-buffer list-buf)
  (sql-datum--admin-request-display "jobs" conn "detail")
  (sql-datum--admin-show-panel (test-admin-display--panel "jobs") conn)
  (test-admin-display-assert "the list's own refresh does not answer for it"
                             (equal (test-admin-display--selected) list-buf))
  (sql-datum--admin-show-detail (test-admin-display--tree) conn)
  (test-admin-display-assert "and the tree still arrives and is selected"
                             (equal (test-admin-display--selected) tree-buf))

  ;; The request is spent either way.
  (test-admin-display-assert "the request having been used up"
                             (null (buffer-local-value
                                    'sql-datum--admin-display-request conn))))

;; Every entry point that deliberately opens a view has to say so, or
;; pressing its key on an already-built buffer does nothing.
(let ((conn test-admin-display--connection))
  (dolist (probe '(("detail"  sql-datum-admin-detail       ("jobs" . "detail"))
                   ("history" sql-datum-admin-job-history  ("jobs" . "history"))))
    (with-current-buffer "*datum-admin:jobs [display-test]*"
      (with-current-buffer conn (setq sql-datum--admin-display-request nil))
      (goto-char (point-min))
      (let ((pos (next-single-property-change (point-min) 'sql-datum-row-id)))
        (when pos (goto-char pos)))
      (cl-letf (((symbol-function 'sql-datum--admin-send-command)
                 (lambda (_cmd) nil)))
        (funcall (nth 1 probe)))
      (test-admin-display-assert
       (format "%s asks for what it opens" (nth 0 probe))
       (equal (buffer-local-value 'sql-datum--admin-display-request conn)
              (nth 2 probe))))))

(message "\n=== a panel dispatches to its own connection ===")

;; `sql-find-sqli-buffer' prefers the current buffer's own connection and
;; falls back to sql.el's global default -- the connection opened most
;; recently.  An admin panel has no `sql-buffer' of its own, so opening
;; one panel from another went to whichever server was connected to
;; last: press the filesystem key in prod's jobs panel and browse dev.

(let ((prod (get-buffer-create "*SQL: dispatch-prod*"))
      (dev  (get-buffer-create "*SQL: dispatch-dev*")))
  (dolist (b (list prod dev))
    (with-current-buffer b
      (setq-local sql-datum--meta (make-hash-table :test #'equal))
      (puthash "server" (if (eq b prod) "prod-sql-01" "dev-sql-09")
               sql-datum--meta)))
  ;; dev was connected last, so it is what sql.el answers with.
  (cl-letf (((symbol-function 'sql-find-sqli-buffer)
             (lambda (&rest _) (buffer-name dev)))
            ((symbol-function 'get-buffer-process) (lambda (b) (and b t))))
    (let (sent)
      (cl-letf (((symbol-function 'sql-datum--enqueue-one)
                 (lambda (cmd &rest _) (push (cons (buffer-name) cmd) sent))))
        ;; A panel of prod's, asking for something with no connection
        ;; named -- which is how the panel entry points send.
        (sql-datum--admin-show-panel (test-admin-display--panel "jobs") prod)
        (with-current-buffer (sql-datum--admin-buffer-name "jobs" nil prod)
          (setq sent nil)
          (sql-datum--admin-send-command-to nil ":admin filesystem")
          (test-admin-display-assert
           "a panel's own connection answers, not the newest one"
           (equal (car (car sent)) (buffer-name prod)))
          (test-admin-display-assert
           "and it is the connection the request was filed against"
           (eq (sql-datum--admin-connection nil) prod)))

        ;; Away from any panel there is nothing better than sql.el's
        ;; answer, and that is what is used.
        (with-temp-buffer
          (setq sent nil)
          (sql-datum--admin-send-command-to nil ":admin filesystem")
          (test-admin-display-assert
           "elsewhere, sql.el still decides"
           (equal (car (car sent)) (buffer-name dev))))

        ;; A connection named outright always wins.
        (with-current-buffer (sql-datum--admin-buffer-name "jobs" nil prod)
          (setq sent nil)
          (sql-datum--admin-send-command-to dev ":admin filesystem")
          (test-admin-display-assert
           "a named connection is used as given"
           (equal (car (car sent)) (buffer-name dev)))))))
  (dolist (b (list prod dev (get-buffer (sql-datum--admin-buffer-name
                                         "jobs" nil prod))))
    (when (buffer-live-p b) (kill-buffer b))))

(message "\n=== each connection keeps its own metadata ===")

;; `defvar-local' with a `make-hash-table' initial value puts ONE table
;; in the variable's default, shared by every buffer that never rebinds
;; it -- and none did.  So two connections wrote their metadata into the
;; same table, the second overwrote the first, and a panel on server one
;; read the name of whichever server connected last.

(let ((one (get-buffer-create "*SQL: meta-one*"))
      (two (get-buffer-create "*SQL: meta-two*")))
  ;; Exactly how it happens: each connection reports in its own buffer.
  (with-current-buffer one
    (sql-datum--handle-envelope "meta" "server:server-ONE"))
  (with-current-buffer two
    (sql-datum--handle-envelope "meta" "server:server-TWO"))

  (test-admin-display-assert
   "the two connections do not share one table"
   (not (eq (buffer-local-value 'sql-datum--meta one)
            (buffer-local-value 'sql-datum--meta two))))
  (test-admin-display-assert
   "the first connection still knows its own server"
   (equal (sql-datum--admin-connected-to one) "server-ONE"))
  (test-admin-display-assert
   "and the second knows its own"
   (equal (sql-datum--admin-connected-to two) "server-TWO"))

  ;; The completion caches were shared the same way, which offered one
  ;; server's columns while talking to another.
  (puthash "dbo.orders" '("id" "total")
           (sql-datum--connection-hash 'sql-datum--columns one))
  (test-admin-display-assert
   "one connection's columns do not leak into another"
   (null (gethash "dbo.orders"
                  (sql-datum--connection-hash 'sql-datum--columns two))))
  (test-admin-display-assert
   "while its own are still there"
   (equal (gethash "dbo.orders"
                   (sql-datum--connection-hash 'sql-datum--columns one))
          '("id" "total")))

  ;; Reading before a connection has said anything must answer "nothing
  ;; known", not fail and not hand back a table someone else writes to.
  (let ((fresh (get-buffer-create "plain.sql")))
    (test-admin-display-assert
     "a buffer that has not connected reads as empty"
     (null (gethash "dbo.orders"
                    (sql-datum--connection-hash 'sql-datum--columns fresh))))
    (test-admin-display-assert
     "and with no buffer at all, likewise"
     (hash-table-p (sql-datum--connection-hash 'sql-datum--columns nil)))
    (kill-buffer fresh))

  ;; Every one of them, not just the two that were noticed.
  (dolist (symbol sql-datum--per-connection-tables)
    (test-admin-display-assert
     (format "%s is the connection's own" symbol)
     (not (eq (buffer-local-value symbol one)
              (buffer-local-value symbol two)))))
  (dolist (b (list one two)) (kill-buffer b)))

(message "\n=== q closes a panel for good ===")

;; `q\=' stops the panel's timer and kills its buffer, but a refresh the
;; timer had already sent is on its way regardless.  It arrives to find
;; no buffer, which the display code reads as a panel being opened for
;; the first time -- and pops it up again.  So quitting a panel while it
;; was refreshing closed it and then reopened it.

(defun test-admin-display--panels ()
  "Return the admin panel buffers that exist."
  (seq-filter (lambda (n) (string-prefix-p "*datum-admin:" n))
              (mapcar #'buffer-name (buffer-list))))

(defun test-admin-display--arrive (json conn)
  "Deliver JSON on CONN the way the process filter does.

Through the envelope handler, not straight into the renderer: a
refresh arriving after `q\=' is turned away there, which is the only
place that can tell an answer nobody is waiting for from a redraw."
  (with-current-buffer conn (sql-datum--handle-admin-panel json))
  (sit-for 0.05))

(defun test-admin-display--activity-json ()
  (concat "{\"panel\":\"activity\",\"headers\":[\"A\",\"B\"],"
          "\"rows\":[[\"1\",\"x\"]],\"row_id\":0,"
          "\"actions\":[],\"info\":null}"))

(defun test-admin-display--tree-json ()
  (concat "{\"panel\":\"jobs\",\"sub_panel\":\"detail\","
          "\"title\":\"Job: nightly load\","
          "\"sections\":[{\"title\":\"Steps\",\"headers\":[\"Step\"],"
          "\"rows\":[[\"1\"]],\"row_id\":0,\"actions\":[]}],"
          "\"info\":null,\"context\":{\"job_name\":\"nightly load\"}}"))

(let ((conn test-admin-display--connection)
      (panel "*datum-admin:activity [display-test]*"))
  (dolist (n (test-admin-display--panels)) (kill-buffer n))
  (with-current-buffer conn
    (setq sql-datum--admin-display-request nil
          sql-datum--admin-quit-requests nil))

  (test-admin-display--show "activity" t)
  (test-admin-display-assert "the panel is open and polling"
                             (and (get-buffer panel)
                                  (buffer-local-value 'sql-datum--admin-timer
                                                      (get-buffer panel))))
  (with-current-buffer panel
    (switch-to-buffer (current-buffer))
    (sql-datum-admin-quit))
  (test-admin-display-assert "q closes it" (null (get-buffer panel)))

  ;; The refresh that was already in flight.
  (test-admin-display--arrive (test-admin-display--activity-json) conn)
  (test-admin-display-assert "a refresh already in flight does not reopen it"
                             (null (get-buffer panel)))
  ;; However many were in flight.
  (test-admin-display--arrive (test-admin-display--activity-json) conn)
  (test-admin-display-assert "nor does a second one"
                             (null (get-buffer panel)))

  ;; Asking for it again is a different matter.
  (test-admin-display--show "activity" t)
  (test-admin-display-assert "asking for it again opens it"
                             (and (get-buffer panel) t))

  ;; One panel quit must not silence another, nor the same panel on
  ;; another connection.
  (with-current-buffer panel
    (switch-to-buffer (current-buffer))
    (sql-datum-admin-quit))
  (test-admin-display--show "jobs" t)
  (test-admin-display-assert "quitting one panel does not silence another"
                             (and (get-buffer
                                   "*datum-admin:jobs [display-test]*") t))
  (let ((other (test-admin-display--connect "quit-other" "other-sql")))
    (sql-datum--admin-request-display "activity" other)
    (test-admin-display--arrive (test-admin-display--activity-json) other)
    (test-admin-display-assert
     "nor the same panel on another connection"
     (and (get-buffer (sql-datum--admin-buffer-name "activity" nil other)) t))
    (dolist (n (list (sql-datum--admin-buffer-name "activity" nil other)))
      (when (get-buffer n) (kill-buffer n)))
    (kill-buffer other))

  ;; A sub-panel is its own key: quitting a job's tree must not turn away
  ;; the list it was opened from.
  (with-current-buffer conn
    (setq sql-datum--admin-quit-requests nil))
  (sql-datum--admin-request-display "jobs" conn "detail")
  (sql-datum--admin-show-detail (test-admin-display--tree) conn)
  (let ((tree "*datum-admin:jobs:detail [display-test]*"))
    (with-current-buffer tree
      (switch-to-buffer (current-buffer))
      (sql-datum-admin-quit))
    (test-admin-display-assert "quitting the tree closes the tree"
                               (null (get-buffer tree)))
    (test-admin-display--show "jobs" t)
    (test-admin-display-assert "and leaves the list alone"
                               (and (get-buffer
                                     "*datum-admin:jobs [display-test]*") t))
    (test-admin-display--arrive (test-admin-display--tree-json) conn)
    (test-admin-display-assert "the tree's own refresh is still turned away"
                               (null (get-buffer tree))))

  ;; A wizard form is an answer to something asked for a moment ago,
  ;; never to a timer, so it is never turned away -- otherwise quitting
  ;; a panel would quietly swallow the next wizard opened on it.
  (with-current-buffer conn (setq sql-datum--admin-quit-requests nil))
  (test-admin-display--show "activity" t)
  (with-current-buffer panel
    (switch-to-buffer (current-buffer))
    (sql-datum-admin-quit))
  (test-admin-display--arrive
   (concat "{\"panel\":\"activity\",\"sub_panel\":\"form\","
           "\"title\":\"t\",\"form\":{\"fields\":[{\"key\":\"n\","
           "\"label\":\"N\",\"type\":\"string\",\"default\":\"\"}],"
           "\"values\":{},\"submit_action\":\"x\",\"notes\":[]}}")
   conn)
  (test-admin-display-assert "a wizard still opens after its panel was quit"
                             (and (get-buffer "*datum-admin:activity-form*") t))

  (dolist (n (test-admin-display--panels)) (kill-buffer n))
  (with-current-buffer conn
    (setq sql-datum--admin-quit-requests nil
          sql-datum--admin-display-request nil)))

(message "\n%d passed, %d failed"
         test-admin-display--pass test-admin-display--fail)
(when (> test-admin-display--fail 0)
  (kill-emacs 1))

(provide 'test-admin-display)
;;; test-admin-display.el ends here
