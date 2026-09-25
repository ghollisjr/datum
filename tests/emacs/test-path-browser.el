;;; test-path-browser.el --- Batch tests for the server path browser  -*- lexical-binding: t; -*-

;; Data and log files live on the database server, which is usually not
;; the machine Emacs runs on, so `read-file-name' would browse the wrong
;; filesystem.  These tests cover the browser that asks the server to
;; enumerate its own directories, and the round trip that puts the chosen
;; path back into the form without losing what was already typed.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-path-browser.el

;;; Code:

(require 'cl-lib)

(defvar test-path-browser--pass 0)
(defvar test-path-browser--fail 0)
(defvar test-path-browser--sent nil
  "Last command handed to the connection, captured instead of sent.")

(defmacro test-path-browser-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-path-browser--pass (1+ test-path-browser--pass))
             (message "  PASS: %s" ,description))
         (setq test-path-browser--fail (1+ test-path-browser--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-path-browser--fail (1+ test-path-browser--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(advice-add 'sql-datum--admin-send-command-to :override
            (lambda (_buf cmd) (setq test-path-browser--sent cmd)))

;; A listing as the databases panel builds it: [is-dir, name, full path],
;; with the parent first.
(defconst test-path-browser--listing
  (concat
   "{\"panel\":\"databases\",\"sub_panel\":\"path-browser\","
   "\"title\":\"Browse: /var/opt/mssql/data\","
   "\"headers\":[\"Dir\",\"Name\",\"Path\"],"
   "\"rows\":[[\"/\",\"..\",\"/var/opt/mssql\"],"
   "[\"/\",\"backups\",\"/var/opt/mssql/data/backups\"],"
   "[\"\",\"master.mdf\",\"/var/opt/mssql/data/master.mdf\"]],"
   "\"row_id\":2,\"actions\":[],"
   "\"info\":\"RET opens a directory, s selects it, q returns\","
   "\"context\":{\"path\":\"/var/opt/mssql/data\",\"field\":\"data_dir\","
   "\"values\":{\"name\":\"wip_db\",\"owner\":\"sa\"},"
   "\"return_action\":\"new-database\"}}"))

(defun test-path-browser--render ()
  "Render a fresh browser buffer and return it."
  (sql-datum--admin-show-path-browser
   (sql-datum--admin-denull-alist
    (json-parse-string test-path-browser--listing
                       :object-type 'alist :array-type 'list))
   nil)
  (get-buffer "*datum-admin:path-browser*"))

(defun test-path-browser--sent-values ()
  "Return the values alist from the last captured command."
  (let ((b64 (car (last (split-string test-path-browser--sent " ")))))
    (alist-get 'values
               (json-parse-string (base64-decode-string b64)
                                  :object-type 'alist :array-type 'list))))

(defun test-path-browser--goto (name)
  "Move point to the row displaying NAME."
  (goto-char (point-min))
  (search-forward name)
  (beginning-of-line))

(message "\n=== Server path browser ===")

;; --- rendering and keys ---
(with-current-buffer (test-path-browser--render)
  (test-path-browser-assert "directories are marked"
                            (save-excursion
                              (test-path-browser--goto "backups")
                              (get-text-property (point) 'sql-datum-is-dir)))
  (test-path-browser-assert "files are not marked as directories"
                            (save-excursion
                              (test-path-browser--goto "master.mdf")
                              (not (get-text-property (point)
                                                      'sql-datum-is-dir))))
  (test-path-browser-assert "parent row is flagged by name, not by path"
                            (save-excursion
                              (test-path-browser--goto "..")
                              (get-text-property (point)
                                                 'sql-datum-is-parent)))
  (test-path-browser-assert "full paths are carried on each row"
                            (save-excursion
                              (test-path-browser--goto "backups")
                              (equal (get-text-property
                                      (point) 'sql-datum-row-id)
                                     "/var/opt/mssql/data/backups")))
  (dolist (binding '(("RET" . sql-datum-path-browser-open)
                     ("^"   . sql-datum-path-browser-up)
                     ("s"   . sql-datum-path-browser-select)
                     ("q"   . sql-datum-path-browser-cancel)))
    (test-path-browser-assert (format "%s is bound" (car binding))
                              (eq (key-binding (kbd (car binding)))
                                  (cdr binding)))))

;; --- selecting ---
;; Selecting kills the browser buffer, so each probe re-renders.
(with-current-buffer (test-path-browser--render)
  (test-path-browser--goto "backups")
  (sql-datum-path-browser-select))
(test-path-browser-assert "a subdirectory row selects that directory"
                          (equal (alist-get 'data_dir
                                            (test-path-browser--sent-values))
                                 "/var/opt/mssql/data/backups"))

(with-current-buffer (test-path-browser--render)
  (test-path-browser--goto "..")
  (sql-datum-path-browser-select))
(test-path-browser-assert "the parent row selects the listed directory"
                          (equal (alist-get 'data_dir
                                            (test-path-browser--sent-values))
                                 "/var/opt/mssql/data"))

(with-current-buffer (test-path-browser--render)
  (test-path-browser--goto "master.mdf")
  (sql-datum-path-browser-select))
(test-path-browser-assert "a file row selects the listed directory"
                          (equal (alist-get 'data_dir
                                            (test-path-browser--sent-values))
                                 "/var/opt/mssql/data"))

(with-current-buffer (test-path-browser--render)
  (sql-datum-path-browser-select))
(test-path-browser-assert "values typed before browsing are preserved"
                          (let ((v (test-path-browser--sent-values)))
                            (and (equal (alist-get 'name v) "wip_db")
                                 (equal (alist-get 'owner v) "sa"))))
(test-path-browser-assert "the form is rebuilt by the recorded action"
                          (string-match-p "new-database"
                                          test-path-browser--sent))

;; --- cancelling ---
(with-current-buffer (test-path-browser--render)
  (sql-datum-path-browser-cancel))
(test-path-browser-assert "cancel leaves the path field unset"
                          (null (alist-get 'data_dir
                                           (test-path-browser--sent-values))))
(test-path-browser-assert "cancel still restores the other values"
                          (equal (alist-get 'name
                                            (test-path-browser--sent-values))
                                 "wip_db"))

;; --- navigating ---
(with-current-buffer (test-path-browser--render)
  (test-path-browser--goto "backups")
  (sql-datum-path-browser-open))
(test-path-browser-assert "opening a directory re-lists it"
                          (string-match-p "browse-path"
                                          test-path-browser--sent))
(test-path-browser-assert "the descended path is sent"
                          (equal (alist-get
                                  'path
                                  (json-parse-string
                                   (base64-decode-string
                                    (car (last (split-string
                                                test-path-browser--sent " "))))
                                   :object-type 'alist :array-type 'list))
                                 "/var/opt/mssql/data/backups"))

(with-current-buffer (test-path-browser--render)
  (test-path-browser--goto "master.mdf")
  (test-path-browser-assert "opening a file is refused"
                            (condition-case nil
                                (progn (sql-datum-path-browser-open) nil)
                              (error t))))

(message "\n%d passed, %d failed"
         test-path-browser--pass test-path-browser--fail)
(when (> test-path-browser--fail 0)
  (kill-emacs 1))

(provide 'test-path-browser)
;;; test-path-browser.el ends here
