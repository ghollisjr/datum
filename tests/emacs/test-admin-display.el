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

(message "\n%d passed, %d failed"
         test-admin-display--pass test-admin-display--fail)
(when (> test-admin-display--fail 0)
  (kill-emacs 1))

(provide 'test-admin-display)
;;; test-admin-display.el ends here
