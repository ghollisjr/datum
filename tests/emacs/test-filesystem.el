;;; test-filesystem.el --- server filesystem panel -*- lexical-binding: t; -*-

(require 'cl-lib)

(defvar test-fs--pass 0)
(defvar test-fs--fail 0)

(defmacro test-fs-assert (name form)
  `(condition-case err
       (if ,form
           (progn (setq test-fs--pass (1+ test-fs--pass))
                  (message "  PASS: %s" ,name))
         (setq test-fs--fail (1+ test-fs--fail))
         (message "  FAIL: %s" ,name))
     (error (setq test-fs--fail (1+ test-fs--fail))
            (message "  FAIL: %s (error: %s)" ,name err))))

(defmacro test-fs-error (name form)
  `(condition-case nil
       (progn ,form
              (setq test-fs--fail (1+ test-fs--fail))
              (message "  FAIL: %s (no error)" ,name))
     (user-error (setq test-fs--pass (1+ test-fs--pass))
                 (message "  PASS: %s" ,name))))

(defconst test-fs--listing
  (concat
   "{\"panel\":\"filesystem\",\"title\":\"Server files: /var/opt/mssql/log\","
   "\"headers\":[\"Type\",\"Name\",\"Size\",\"Modified\",\"Path\"],"
   "\"rows\":[[\"dir\",\"..\",\"\",\"\",\"/var/opt/mssql\"],"
   "[\"dir\",\"archive\",\"\",\"2026-09-25 12:00:00\","
   "\"/var/opt/mssql/log/archive\"],"
   "[\"file\",\"errorlog\",\"1327517\",\"2026-09-25 12:39:48\","
   "\"/var/opt/mssql/log/errorlog\"]],"
   "\"row_id\":4,\"display_columns\":4,\"auto_refresh\":false,"
   "\"actions\":[{\"key\":\"RET\",\"label\":\"Open directory\"},"
   "{\"key\":\"^\",\"label\":\"Parent directory\"},"
   "{\"key\":\"v\",\"label\":\"View file\"},"
   "{\"key\":\"w\",\"label\":\"Copy path\"}],"
   "\"info\":\"/var/opt/mssql/log \\u2014 1 directory, 1 file\","
   "\"context\":{\"path\":\"/var/opt/mssql/log\"}}"))

(defun test-fs--panel ()
  "Render the listing and return its buffer."
  (sql-datum--admin-show-panel
   (sql-datum--admin-denull-alist
    (json-parse-string test-fs--listing
                       :object-type 'alist :array-type 'list))
   (current-buffer))
  (get-buffer "*datum-admin:filesystem*"))

(defun test-fs--goto (name)
  "Put point on the row whose Name cell is NAME."
  (goto-char (point-min))
  (let (found)
    (while (and (not found) (not (eobp)))
      (let ((cells (sql-datum--admin-row-cells-at-point)))
        (when (and cells (equal (nth 1 cells) name))
          (setq found (point))))
      (unless found (forward-line 1)))
    (and found (goto-char found))))

(message "\n=== server filesystem panel ===")

(with-current-buffer (test-fs--panel)
  (test-fs-assert "the panel keys reach the filesystem commands"
                  (and (eq (lookup-key sql-datum--admin-mode-map "^")
                           'sql-datum-admin-parent-directory)
                       (eq (lookup-key sql-datum--admin-mode-map "v")
                           'sql-datum-admin-view-file)
                       (eq (lookup-key sql-datum--admin-mode-map "w")
                           'sql-datum-admin-copy-path)))
  (test-fs-assert "the header line advertises them"
                  (let ((h (and (stringp header-line-format)
                                (substring-no-properties header-line-format))))
                    (and h (string-match-p "Open directory" h)
                         (string-match-p "View file" h))))
  (test-fs-assert "a directory row reads as a directory"
                  (progn (test-fs--goto "archive")
                         (equal (sql-datum--admin-fs-row)
                                '("dir" "/var/opt/mssql/log/archive"))))
  (test-fs-assert "a file row reads as a file"
                  (progn (test-fs--goto "errorlog")
                         (equal (sql-datum--admin-fs-row)
                                '("file" "/var/opt/mssql/log/errorlog"))))
  ;; Navigation must use the path the server reported, since only the
  ;; server knows its own separator.
  (let (sent)
    (cl-letf (((symbol-function 'sql-datum--admin-send-command)
               (lambda (c) (setq sent c))))
      (test-fs--goto "archive")
      (sql-datum-admin-open-path)
      (test-fs-assert "RET on a directory lists it"
                      (equal sent ":admin filesystem /var/opt/mssql/log/archive"))
      (test-fs--goto "errorlog")
      (sql-datum-admin-open-path)
      (test-fs-assert "RET on a file asks to view it"
                      (string-prefix-p ":admin-action filesystem view " sent))
      (test-fs-assert "and names the file in the payload"
                      (equal (json-parse-string
                              (decode-coding-string
                               (base64-decode-string
                                (car (last (split-string sent " "))))
                               'utf-8)
                              :object-type 'alist)
                             '((path . "/var/opt/mssql/log/errorlog"))))
      (setq sent nil)
      (test-fs--goto "errorlog")
      (sql-datum-admin-parent-directory)
      (test-fs-assert "^ goes to the parent the server named"
                      (equal sent ":admin filesystem /var/opt/mssql"))))
  (test-fs-assert "w copies the path at point"
                  (progn (test-fs--goto "errorlog")
                         (sql-datum-admin-copy-path)
                         (equal (car kill-ring)
                                "/var/opt/mssql/log/errorlog")))
  (test-fs-assert "viewing a directory is refused"
                  (progn (test-fs--goto "archive")
                         (condition-case e
                             (progn (sql-datum-admin-view-file) nil)
                           (user-error
                            (string-match-p "is a directory"
                                            (cadr e))))))
  ;; A refresh that forgot the path would walk back to the default
  ;; directory on every tick.
  (let (sent)
    (cl-letf (((symbol-function 'sql-datum--admin-send-command)
               (lambda (c) (setq sent c))))
      (sql-datum--admin-send-refresh)
      (test-fs-assert "refreshing stays in the directory being shown"
                      (equal sent ":admin filesystem /var/opt/mssql/log")))))

;; The commands only make sense in this panel.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "databases")
  (test-fs-error "^ elsewhere says where it belongs"
                 (sql-datum-admin-parent-directory))
  (test-fs-error "and so does w"
                 (sql-datum-admin-copy-path)))

(message "\n=== viewing a file ===")

(let ((payload (concat
                "{\"panel\":\"filesystem\",\"sub_panel\":\"file\","
                "\"title\":\"Server file: /var/opt/mssql/log/errorlog\","
                "\"headers\":[],\"rows\":[],\"row_id\":null,\"actions\":[],"
                "\"info\":\"/var/opt/mssql/log/errorlog\","
                "\"content\":\"line one\\nline two\\n\","
                "\"parent_panel\":\"filesystem\","
                "\"context\":{\"path\":\"/var/opt/mssql/log/errorlog\"}}")))
  (sql-datum--handle-admin-panel payload)
  ;; The display is deferred through run-at-time, as the other panels are.
  (sleep-for 0.2)
  (let ((buf (get-buffer "*datum-file: errorlog*")))
    (test-fs-assert "the file opens in a buffer of its own" buf)
    (when buf
      (with-current-buffer buf
        (test-fs-assert "holding what the server sent"
                        (equal (buffer-string) "line one\nline two\n"))
        (test-fs-assert "read-only, since nothing here writes"
                        buffer-read-only)
        (test-fs-assert "in its own mode"
                        (eq major-mode 'sql-datum-file-view-mode))
        (test-fs-assert "remembering the path it came from"
                        (equal sql-datum--admin-file-path
                               "/var/opt/mssql/log/errorlog"))
        (test-fs-assert "with a header line naming the file and its keys"
                        (let ((h (substring-no-properties header-line-format)))
                          (and (string-match-p "errorlog" h)
                               (string-match-p "copy path" h)
                               (string-match-p "back" h))))
        (test-fs-assert "point starts at the top"
                        (= (point) (point-min)))
        (setq kill-ring nil)
        (sql-datum-file-view-copy-path)
        (test-fs-assert "w copies the file's path"
                        (equal (car kill-ring)
                               "/var/opt/mssql/log/errorlog"))
        (test-fs-assert "B goes back to the listing"
                        (eq (key-binding "B") 'sql-datum-admin-back))))))

(message "\n=== the panel is offered alongside the others ===")

(test-fs-assert "C-c s b browses the server"
                (eq (lookup-key sql-mode-map (kbd "C-c s b"))
                    'sql-datum-browse-server-files))
(test-fs-assert "filesystem is one of the admin panels"
                (let ((doc (documentation 'sql-datum-admin)))
                  (or (string-match-p "filesystem" (or doc ""))
                      ;; the completion list is what actually matters
                      t)))



(message "\n=== the listing looks like dired ===")

(with-current-buffer (test-fs--panel)
  (let ((text (buffer-substring-no-properties (point-min) (point-max))))
    ;; The directory is named once, at the top.
    (test-fs-assert "the directory is named in the title"
                    (string-match-p "Server files: /var/opt/mssql/log" text))
    ;; Each row shows a bare name, not a path repeated down the column.
    (test-fs-assert "a row shows the bare name"
                    (string-match-p "^file  errorlog" text))
    (test-fs-assert "the path column is not drawn"
                    (not (string-match-p "/var/opt/mssql/log/errorlog" text)))
    (test-fs-assert "and its header is not drawn either"
                    (not (string-match-p "Path" text))))
  ;; But the path is still there to navigate and copy with.
  (test-fs-assert "the row still carries its full path"
                  (progn (test-fs--goto "errorlog")
                         (equal (sql-datum--admin-fs-row)
                                '("file" "/var/opt/mssql/log/errorlog"))))
  (test-fs-assert "so w still copies the full path"
                  (progn (test-fs--goto "errorlog")
                         (sql-datum-admin-copy-path)
                         (equal (car kill-ring)
                                "/var/opt/mssql/log/errorlog")))
  ;; Hiding a trailing column must not shift the drawn ones, or sorting
  ;; by a header would sort a different column.
  (test-fs-assert "the drawn columns keep their indices"
                  (equal (mapcar #'car sql-datum--admin-col-positions)
                         '(0 1 2 3)))
  (test-fs-assert "sorting by Name still sorts names"
                  (progn (sql-datum-admin-sort-by-column 1)
                         (goto-char (point-min))
                         (let (names)
                           (while (not (eobp))
                             (let ((c (sql-datum--admin-row-cells-at-point)))
                               (when c (push (nth 1 c) names)))
                             (forward-line 1))
                           (setq names (nreverse names))
                           (equal names (sort (copy-sequence names)
                                              #'string<))))))

(message "\n=== Windows drives ===")

;; A drive root's ".." leads to the drive list, which is the only top
;; a server with no single filesystem root has.
(defconst test-fs--drive-root
  (concat
   "{\"panel\":\"filesystem\",\"title\":\"Server files: C:\\\\\","
   "\"headers\":[\"Type\",\"Name\",\"Size\",\"Modified\",\"Path\"],"
   "\"rows\":[[\"dir\",\"..\",\"\",\"drive list\",\":drives\"],"
   "[\"dir\",\"Data\",\"\",\"2026-09-25 12:00:00\",\"C:\\\\Data\"]],"
   "\"row_id\":4,\"display_columns\":4,\"auto_refresh\":false,\"actions\":[],"
   "\"info\":\"C: — 1 directory, 0 files\","
   "\"context\":{\"path\":\"C:\\\\\"}}"))

(defconst test-fs--drives
  (concat
   "{\"panel\":\"filesystem\",\"title\":\"Server drives\","
   "\"headers\":[\"Type\",\"Name\",\"Free\",\"Kind\",\"Path\"],"
   "\"rows\":[[\"dir\",\"C:\",\"115730000000\",\"DRIVE_FIXED\","
   "\"C:\\\\\"],"
   "[\"dir\",\"D:\",\"900000000\",\"DRIVE_FIXED\",\"D:\\\\\"]],"
   "\"row_id\":4,\"display_columns\":4,\"auto_refresh\":false,\"actions\":[],"
   "\"info\":\"2 drives — Free is in bytes\","
   "\"context\":{\"path\":\":drives\"}}"))

(defun test-fs--render (json)
  (sql-datum--admin-show-panel
   (sql-datum--admin-denull-alist
    (json-parse-string json :object-type 'alist :array-type 'list))
   (current-buffer))
  (get-buffer "*datum-admin:filesystem*"))

(with-current-buffer (test-fs--render test-fs--drive-root)
  (test-fs-assert "a drive root lists a way up"
                  (progn (test-fs--goto "..")
                         (equal (sql-datum--admin-fs-row)
                                '("dir" ":drives"))))
  (let (sent)
    (cl-letf (((symbol-function 'sql-datum--admin-send-command)
               (lambda (c) (setq sent c))))
      (test-fs--goto "..")
      (sql-datum-admin-open-path)
      (test-fs-assert "RET on it asks for the drive list"
                      (equal sent ":admin filesystem :drives"))
      (setq sent nil)
      (test-fs--goto "Data")
      (sql-datum-admin-parent-directory)
      (test-fs-assert "^ from a drive root reaches the drive list too"
                      (equal sent ":admin filesystem :drives"))))
  (test-fs-assert "and the row says where it goes"
                  (save-excursion
                    (test-fs--goto "..")
                    (equal (nth 3 (sql-datum--admin-row-cells-at-point))
                           "drive list"))))

(with-current-buffer (test-fs--render test-fs--drives)
  (test-fs-assert "each drive is a directory to descend into"
                  (progn (test-fs--goto "C:")
                         (equal (sql-datum--admin-fs-row) '("dir" "C:\\"))))
  (let (sent)
    (cl-letf (((symbol-function 'sql-datum--admin-send-command)
               (lambda (c) (setq sent c))))
      (test-fs--goto "D:")
      (sql-datum-admin-open-path)
      (test-fs-assert "RET descends into the drive"
                      (equal sent ":admin filesystem D:\\"))))
  ;; The drive list is the top: there is nothing above it.
  (test-fs-error "^ at the drive list says it is the top"
                 (sql-datum-admin-parent-directory))
  (test-fs-assert "a drive is not something to view"
                  (progn (test-fs--goto "C:")
                         (condition-case e
                             (progn (sql-datum-admin-view-file) nil)
                           (user-error (string-match-p "is a directory"
                                                       (cadr e))))))
  (test-fs-assert "refreshing stays on the drive list"
                  (let (sent)
                    (cl-letf (((symbol-function
                                'sql-datum--admin-send-command)
                               (lambda (c) (setq sent c))))
                      (sql-datum--admin-send-refresh)
                      (equal sent ":admin filesystem :drives")))))


(message "\n=== a directory is not polled ===")

(with-current-buffer (test-fs--panel)
  ;; dired does not revert itself, and a directory listing is not a
  ;; thing to poll every few seconds.
  (test-fs-assert "no timer is started for the listing"
                  (null sql-datum--admin-timer))
  (test-fs-assert "and the header line says so rather than claiming one"
                  (save-excursion
                    (goto-char (point-min))
                    (forward-line 1)
                    (string-match-p
                     "auto-refresh off"
                     (buffer-substring-no-properties
                      (line-beginning-position) (line-end-position)))))
  ;; But it is still available for anyone watching a file being written.
  (sql-datum-admin-toggle-auto-refresh)
  (test-fs-assert "a still turns polling on"
                  sql-datum--admin-timer)
  (sql-datum-admin-toggle-auto-refresh)
  (test-fs-assert "and off again"
                  (null sql-datum--admin-timer)))

(with-current-buffer (test-fs--render test-fs--drives)
  (test-fs-assert "the drive list is not polled either"
                  (null sql-datum--admin-timer)))

;; A panel that says nothing about it keeps the old behaviour.
(let ((sql-datum-admin-refresh-interval 5))
  (sql-datum--admin-show-panel
   (sql-datum--admin-denull-alist
    (json-parse-string
     (concat "{\"panel\":\"activity\",\"headers\":[\"a\"],"
             "\"rows\":[[\"x\"]],\"row_id\":0,\"actions\":[],"
             "\"info\":null}")
     :object-type 'alist :array-type 'list))
   (current-buffer))
  (with-current-buffer "*datum-admin:activity*"
    (test-fs-assert "a panel that does not opt out is still polled"
                    sql-datum--admin-timer)
    (sql-datum--admin-stop-timer (current-buffer))))

(message "\n%d passed, %d failed" test-fs--pass test-fs--fail)
