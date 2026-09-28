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
       (equal (buffer-local-value 'sql-datum--admin-display-request prod)
              "databases"))
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

(message "\n%d passed, %d failed"
         test-admin-display--pass test-admin-display--fail)
(when (> test-admin-display--fail 0)
  (kill-emacs 1))

(provide 'test-admin-display)
;;; test-admin-display.el ends here
