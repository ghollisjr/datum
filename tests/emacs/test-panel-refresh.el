;;; test-panel-refresh.el --- Batch tests for panel refresh after actions  -*- lexical-binding: t; -*-

;; Only top-level panels get an auto-refresh timer, so a sub-panel such
;; as the file list or a login's database users showed whatever was true
;; when it was opened and never updated.  Mapping a login into a database
;; left the mapping invisible until the buffer was closed and reopened.
;;
;; The server now re-sends the affected panel after a mutating action.
;; These tests pin that the resent panel lands in the existing buffer,
;; updates it, and does not steal focus the way a fresh request does.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-panel-refresh.el

;;; Code:

(require 'cl-lib)

(defvar test-panel-refresh--pass 0)
(defvar test-panel-refresh--fail 0)

(defmacro test-panel-refresh-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-panel-refresh--pass (1+ test-panel-refresh--pass))
             (message "  PASS: %s" ,description))
         (setq test-panel-refresh--fail (1+ test-panel-refresh--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-panel-refresh--fail (1+ test-panel-refresh--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(defun test-panel-refresh--mappings (rows)
  "Deliver a user-mappings panel whose rows are ROWS (a JSON array)."
  (sql-datum--admin-show-panel
   (sql-datum--admin-denull-alist
    (json-parse-string
     (concat "{\"panel\":\"security\",\"sub_panel\":\"user-mappings\","
             "\"title\":\"Database Users: bob\","
             "\"headers\":[\"Database\",\"User\",\"Database Roles\"],"
             "\"rows\":" rows ",\"row_id\":0,\"actions\":[],"
             "\"info\":null,\"parent_panel\":\"security\","
             "\"context\":{\"login\":\"bob\"}}")
     :object-type 'alist :array-type 'list))
   nil)
  (get-buffer (sql-datum--admin-buffer-name "security" "user-mappings")))

(message "\n=== Panel refresh after an action ===")

(setq sql-datum--admin-display-request nil)
(let ((buf (test-panel-refresh--mappings "[]")))
  (test-panel-refresh-assert "the mapping panel opens"
                             (buffer-live-p buf))
  (with-current-buffer buf
    (test-panel-refresh-assert "it starts with no mappings"
                               (not (string-match-p "datum_test"
                                                    (buffer-string))))
    (test-panel-refresh-assert "the login context is kept"
                               (equal (alist-get 'login
                                                 sql-datum--admin-context)
                                      "bob"))))

;; The server re-sends the panel after add-mapping.
(let ((buf (test-panel-refresh--mappings
            "[[\"datum_test\",\"bob\",\"\"]]")))
  (with-current-buffer buf
    (test-panel-refresh-assert "a new mapping appears without reopening"
                               (string-match-p "datum_test"
                                               (buffer-string)))))

;; And again after the roles change.
(let ((buf (test-panel-refresh--mappings
            "[[\"datum_test\",\"bob\",\"db_datareader\"]]")))
  (with-current-buffer buf
    (test-panel-refresh-assert "a role change appears without reopening"
                               (string-match-p "db_datareader"
                                               (buffer-string)))))

;; And the row goes away after remove-mapping.
(let ((buf (test-panel-refresh--mappings "[]")))
  (with-current-buffer buf
    (test-panel-refresh-assert "a removed mapping disappears"
                               (not (string-match-p "datum_test"
                                                    (buffer-string))))))

(test-panel-refresh-assert
 "the refresh reuses one buffer rather than making more"
 (= 1 (length (seq-filter
               (lambda (b)
                 (string-match-p "user-mappings" (buffer-name b)))
               (buffer-list)))))

;; A refresh must not drag a buried panel back into a window: only an
;; explicit request does that.
(let ((buf (get-buffer (sql-datum--admin-buffer-name
                        "security" "user-mappings"))))
  (delete-windows-on buf)
  (test-panel-refresh--mappings "[[\"datum_test\",\"bob\",\"\"]]")
  (test-panel-refresh-assert "a refresh does not resurrect a buried panel"
                             (not (get-buffer-window buf t)))
  (with-current-buffer buf
    (test-panel-refresh-assert "but it still updated the buffer"
                               (string-match-p "datum_test"
                                               (buffer-string)))))

;; --- g reconstructs the right command for the new sub-panels ---

(defun test-panel-refresh--refresh-command (panel sub context-json)
  "Return the command `g' would send for PANEL/SUB with CONTEXT-JSON."
  (with-temp-buffer
    (setq-local sql-datum--admin-panel-name panel)
    (setq-local sql-datum--admin-panel-data
                (sql-datum--admin-denull-alist
                 (json-parse-string (format "{\"sub_panel\":\"%s\"}" sub)
                                    :object-type 'alist :array-type 'list)))
    (setq-local sql-datum--admin-context
                (sql-datum--admin-denull-alist
                 (json-parse-string context-json
                                    :object-type 'alist :array-type 'list)))
    (let (sent)
      (cl-letf (((symbol-function 'sql-datum--admin-send-command)
                 (lambda (cmd) (setq sent cmd))))
        (sql-datum--admin-send-refresh))
      sent)))

(test-panel-refresh-assert
 "g refreshes a file list in place"
 (equal (test-panel-refresh--refresh-command
         "databases" "files" "{\"database\":\"appdb\"}")
        ":admin-action databases files appdb"))

(test-panel-refresh-assert
 "g refreshes a user-mapping list in place"
 (equal (test-panel-refresh--refresh-command
         "security" "user-mappings" "{\"login\":\"bob\"}")
        ":admin-action security mappings bob"))

(test-panel-refresh-assert
 "g still refreshes a job detail view"
 (equal (test-panel-refresh--refresh-command
         "jobs" "detail" "{\"job_name\":\"Nightly\"}")
        ":admin jobs detail Nightly"))

(test-panel-refresh-assert
 "g falls back to the top-level panel when there is no sub-panel"
 (equal (test-panel-refresh--refresh-command
         "databases" "none" "{\"database\":\"appdb\"}")
        ":admin databases"))


;; --- the keys must be visible without scrolling ---
;;
;; A panel long enough to scroll put its help off-screen, which is where
;; it is least useful.  The header line is pinned to the top of the
;; window and costs no buffer lines.

(let ((buf (test-panel-refresh--mappings
            "[[\"a\",\"bob\",\"\"],[\"b\",\"bob\",\"\"]]")))
  (with-current-buffer buf
    (test-panel-refresh-assert "the panel sets a header line"
                               (and header-line-format
                                    (stringp header-line-format)))
    (test-panel-refresh-assert "a sub-panel advertises going back"
                               (string-match-p
                                "back" (substring-no-properties
                                        header-line-format)))
    (test-panel-refresh-assert "quitting is advertised"
                               (string-match-p
                                "quit" (substring-no-properties
                                        header-line-format)))
    (test-panel-refresh-assert
     "the header costs no buffer lines, so cursor restore is unaffected"
     (= sql-datum--admin-header-line-count 5))
    (test-panel-refresh-assert
     "the action keys are no longer duplicated below the table"
     (not (string-match-p "Keys:" (buffer-string))))))

;; A top-level panel has no parent, so it must not offer "back".
(let ((buf (progn
             (sql-datum--admin-show-panel
              (sql-datum--admin-denull-alist
               (json-parse-string
                (concat "{\"panel\":\"databases\","
                        "\"headers\":[\"Database\"],"
                        "\"rows\":[[\"appdb\"]],\"row_id\":0,"
                        "\"actions\":[{\"key\":\"N\","
                        "\"label\":\"New database\","
                        "\"command\":\"new-database\"}],"
                        "\"info\":null}")
                :object-type 'alist :array-type 'list))
              nil)
             (get-buffer "*datum-admin:databases*"))))
  (with-current-buffer buf
    (test-panel-refresh-assert "a panel action appears in the header"
                               (string-match-p
                                "New database" (substring-no-properties
                                                header-line-format)))
    (test-panel-refresh-assert "a top-level panel offers no back key"
                               (not (string-match-p
                                     "back" (substring-no-properties
                                             header-line-format))))))

(message "\n%d passed, %d failed"
         test-panel-refresh--pass test-panel-refresh--fail)
(when (> test-panel-refresh--fail 0)
  (kill-emacs 1))

(provide 'test-panel-refresh)
;;; test-panel-refresh.el ends here
