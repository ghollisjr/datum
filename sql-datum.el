;;; sql-datum.el --- Adds Datum as a SQL Product   -*- lexical-binding: t; -*-

;; Copyright (C) 2021-2024 Sebastian Monia
;;
;; Author: Sebastian Monia <smonia@outlook.com>
;; URL: https://github.com/sebasmonia/datum
;; Package-Requires: ((emacs "27.1"))
;; Version: 2.0
;; Keywords: languages processes tools

;; This file is not part of GNU Emacs.

;;; SPDX-License-Identifier: MIT

;;; Commentary:

;; Pre-packaged setup to use Datum as an interface for SQLi buffers.
;; Steps to setup:
;;   1. Place sql-datum.el in your load-path.
;;   2. (require 'sql-datum)
;; Then...
;;   3. M-x sql-datum will prompt for parameters to create a connection
;; - OR -
;;   3. Add to sql-connection-alist an item that uses Datum to connect,
;;      it will show up in the candidates when calling sql-connect.
;;
;; For a detailed Datum user manual, and additional Emacs setup examples, see:
;; https://github.com/sebasmonia/datum/blob/main/README.md

;;; Code:

(require 'sql)
(require 'cl-lib)

;;; ---------------------------------------------------------------------------
;;; Customization
;;; ---------------------------------------------------------------------------

(defcustom sql-datum-program "datum"
  "Command to start Datum.
See https://github.com/sebasmonia/datum for instructions on how to install."
  :type 'file
  :group 'SQL)

(defcustom sql-datum-password-variable "SQLDATUMPASS"
  "Environment variable to store the connection password.
When a name is provided, it is used as temporary storage right before
starting datum, and cleared right after.  If nil, use the \"--pass\"
flag, but then the password is visible for example in `list-processes'."
  :type 'string
  :group 'SQL)

(defcustom sql-datum-populate-completion t
  "When non-nil, introspection results populate `completion-at-point'.
Introspection commands (:tables, :schemas, etc.) automatically update
the completion candidates available when editing SQL."
  :type 'boolean
  :group 'SQL)

(defcustom sql-datum-open-result-file nil
  "When non-nil, offer to open the file after a successful :out export.
By default, export completion is reported in the minibuffer only."
  :type 'boolean
  :group 'SQL)

(defcustom sql-datum-import-batch-size 1000
  "Number of rows per batch for :in imports.
Lower values use less memory; higher values are faster.
Override per-call with a prefix argument to `sql-datum-import'."
  :type 'integer
  :group 'SQL)

(defcustom sql-datum-confirm-drop t
  "When non-nil, prompt for confirmation before dropping a table."
  :type 'boolean
  :group 'SQL)


;;; ---------------------------------------------------------------------------
;;; Buffer-local state
;;; ---------------------------------------------------------------------------

(defvar-local sql-datum--dialect nil
  "Detected SQL dialect for this datum buffer (e.g. \"mssql\", \"postgres\").")

(defvar-local sql-datum--meta (make-hash-table :test #'equal)
  "Metadata for this datum buffer: server, database, user, version.")

(defvar-local sql-datum--tables nil
  "List of table names populated by :tables introspection.")

(defvar-local sql-datum--schemas nil
  "List of schema names populated by :schemas introspection.")

(defvar-local sql-datum--databases nil
  "List of database names populated by :databases introspection.")

(defvar-local sql-datum--routines nil
  "List of routine names populated by :routines introspection.")

(defvar-local sql-datum--columns (make-hash-table :test #'equal)
  "Hash table mapping \"schema.table\" to list of column name strings.")

;;; ---------------------------------------------------------------------------
;;; Envelope protocol
;;; ---------------------------------------------------------------------------

(defconst sql-datum--envelope-re
  "##DATUM:\\([^:]+\\):\\(.*?\\)##"
  "Regexp matching a datum envelope line.
Group 1 is the message type, group 2 is the payload.")

(defvar-local sql-datum--partial-line nil
  "Buffered partial line from a previous filter call.
When comint splits a long output across multiple filter invocations,
an envelope line may arrive in fragments.  We hold the incomplete
fragment here until the next call completes it.")

(defun sql-datum--preoutput-filter (output)
  "Strip envelope lines from OUTPUT and act on them.
Installed as a `comint-preoutput-filter-functions' hook.
Returns OUTPUT with all ##DATUM:...## lines removed.
Handles partial envelope lines split across multiple filter calls."
  (let ((lines (split-string output "\n"))
        (clean-lines nil))
    ;; Prepend any buffered partial line to the first line
    (when (and sql-datum--partial-line lines)
      (setcar lines (concat sql-datum--partial-line (car lines)))
      (setq sql-datum--partial-line nil))
    ;; Check if the last line is partial (output didn't end with newline).
    ;; A partial line is one that looks like it could be the start of an
    ;; envelope but doesn't have the closing ##.
    (let ((last-line (car (last lines))))
      (when (and last-line
                 (not (string-empty-p last-line))
                 (string-match-p "##DATUM:" last-line)
                 (not (string-match-p "##DATUM:[^:]+:.*?##" last-line)))
        ;; Incomplete envelope — buffer it for next call
        (setq sql-datum--partial-line last-line)
        (setq lines (butlast lines))))
    (dolist (line lines)
      (if (string-match sql-datum--envelope-re line)
          (sql-datum--handle-envelope (match-string 1 line)
                                      (match-string 2 line))
        (push line clean-lines)))
    (string-join (nreverse clean-lines) "\n")))

(defun sql-datum--handle-envelope (type payload)
  "Dispatch on envelope TYPE with PAYLOAD."
  (pcase type
    ("info"
     (message "datum: %s" payload))
    ("warn"
     (message "datum warning: %s" payload)
     ;; Persist dialect-unknown warning in mode line via dialect handler
     (when (string-match-p "ANSI SQL" payload)
       (sql-datum--set-dialect "ansi")))
    ("error"
     (message "datum error: %s" payload))
    ("dialect"
     (sql-datum--set-dialect payload))
    ("meta"
     (when (string-match "\\([^:]+\\):\\(.*\\)" payload)
       (let ((key (match-string 1 payload))
             (val (match-string 2 payload)))
         (puthash key val sql-datum--meta)
         (sql-datum--update-mode-line))))
    ("result-file"
     (when (string-match "\\(.*\\):\\([^:]+\\)$" payload)
       (let ((path (match-string 1 payload))
             (fmt  (match-string 2 payload)))
         (sql-datum--handle-result-file path fmt))))
    ("introspect"
     (when (string-match "\\(.*\\):\\(\\[.*\\)" payload)
       (let ((kind (match-string 1 payload))
             (json (match-string 2 payload)))
         (sql-datum--handle-introspect kind json))))
    ("introspect+"
     (when (string-match "\\(.*\\):\\(\\[.*\\)" payload)
       (let ((kind (match-string 1 payload))
             (json (match-string 2 payload)))
         (sql-datum--handle-introspect-append kind json))))
    ("running-text"
     (let ((text (replace-regexp-in-string "\\\\n" "\n" payload)))
       ;; Remember which SQLi buffer sent this, for refresh later
       (setq sql-datum--running-sqli-buf (current-buffer))
       (sql-datum--show-running-queries text)
       (unless sql-datum--running-timer
         (sql-datum--running-start-timer))))
    ("definition"
     (when (string-match "\\([^:]+\\):\\(.*\\)" payload)
       (let ((obj-name (match-string 1 payload))
             (text (replace-regexp-in-string "\\\\n" "\n"
                                             (match-string 2 payload)))
             (sqli-buf (current-buffer)))
         ;; Defer to avoid disrupting comint's process filter context
         (run-at-time 0 nil #'sql-datum--show-definition
                      obj-name text sqli-buf))))))

(defun sql-datum--set-dialect (name)
  "Set the buffer-local dialect to NAME and refresh the mode line."
  (setq sql-datum--dialect name)
  (sql-datum--update-mode-line))

(defun sql-datum--update-mode-line ()
  "Update the mode line to reflect current dialect and metadata."
  (let* ((dialect  (or sql-datum--dialect "?"))
         (server   (gethash "server"   sql-datum--meta ""))
         (database (gethash "database" sql-datum--meta ""))
         (user     (gethash "user"     sql-datum--meta ""))
         (parts    (cl-remove-if #'string-empty-p
                                 (list dialect database server user)))
         (label    (concat "[datum:" (string-join parts ":") "]")))
    (setq mode-name label))
  (force-mode-line-update))

(defun sql-datum--handle-result-file (path fmt)
  "Act on a result-file envelope: report completion in the minibuffer.
When `sql-datum-open-result-file' is non-nil, also offer to open the file."
  (message "datum: export complete — %s (%s)" path fmt)
  (when sql-datum-open-result-file
    (when (y-or-n-p (format "Open %s? " path))
      (find-file path))))

(defun sql-datum--handle-introspect (kind json-str)
  "Update completion state from an introspect envelope."
  (condition-case err
      (let ((items (json-parse-string json-str :array-type 'list)))
        (pcase kind
          ("databases"
           (setq sql-datum--databases items))
          ("schemas"
           (setq sql-datum--schemas items))
          ("tables"
           (setq sql-datum--tables items))
          ("routines"
           (setq sql-datum--routines items))
          ((pred (string-prefix-p "columns:"))
           (let ((table (substring kind (length "columns:"))))
             (puthash table items sql-datum--columns)))))
    (error (message "datum: failed to parse introspect payload: %s" err))))

(defun sql-datum--handle-introspect-append (kind json-str)
  "Append to completion state from a continuation introspect+ envelope."
  (condition-case err
      (let ((items (json-parse-string json-str :array-type 'list)))
        (pcase kind
          ("databases"
           (setq sql-datum--databases (append sql-datum--databases items)))
          ("schemas"
           (setq sql-datum--schemas (append sql-datum--schemas items)))
          ("tables"
           (setq sql-datum--tables (append sql-datum--tables items)))
          ("routines"
           (setq sql-datum--routines (append sql-datum--routines items)))
          ((pred (string-prefix-p "columns:"))
           (let* ((table (substring kind (length "columns:")))
                  (existing (gethash table sql-datum--columns)))
             (puthash table (append existing items) sql-datum--columns)))))
    (error (message "datum: failed to parse introspect+ payload: %s" err))))

(defvar sql-datum--running-timer nil
  "Timer for auto-refreshing the running queries buffer.")

(defvar sql-datum--running-sqli-buf nil
  "The SQLi buffer used for running-queries auto-refresh.")

(defcustom sql-datum-running-refresh-interval 5
  "Seconds between auto-refresh of the running queries buffer.
Set to nil to disable auto-refresh."
  :type '(choice (const :tag "Disabled" nil) integer)
  :group 'SQL)

(defun sql-datum--show-running-queries (text)
  "Display TEXT in a dedicated running-queries buffer.
TEXT is pre-formatted tabular output from the Python printer."
  (let ((buf (get-buffer-create "*datum-running-queries*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (format "datum: Running Queries  (last refresh: %s)"
                        (format-time-string "%H:%M:%S")))
        (if sql-datum-running-refresh-interval
            (insert (format "  [auto-refresh %ds]"
                            sql-datum-running-refresh-interval))
          (insert "  [auto-refresh off]"))
        (insert "\n\n")
        (insert text)
        (insert "\n\n")
        (insert "Press 'g' to refresh, 'a' to toggle auto-refresh, 'q' to quit.\n"))
      (setq truncate-lines t)
      (sql-datum--running-mode))
    (display-buffer buf)))

(defvar sql-datum--running-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map "q" #'sql-datum-running-quit)
    (define-key map "g" #'sql-datum-running-refresh)
    (define-key map "a" #'sql-datum-running-toggle-auto-refresh)
    map)
  "Keymap for the datum running queries buffer.")

(define-derived-mode sql-datum--running-mode special-mode "datum-running"
  "Major mode for the datum running queries buffer."
  (setq buffer-read-only t))

(defun sql-datum-running-refresh ()
  "Manually refresh the running queries display."
  (interactive)
  (sql-datum--send-running))

(defun sql-datum-running-toggle-auto-refresh ()
  "Toggle auto-refresh of the running queries buffer."
  (interactive)
  (if sql-datum--running-timer
      (progn
        (sql-datum--running-stop-timer)
        (message "datum: auto-refresh disabled"))
    (sql-datum--running-start-timer)
    (message "datum: auto-refresh enabled (%ds interval)"
             sql-datum-running-refresh-interval)))

(defun sql-datum-running-quit ()
  "Stop auto-refresh and close the running queries buffer."
  (interactive)
  (sql-datum--running-stop-timer)
  (quit-window t))

(defun sql-datum--send-running ()
  "Send :running to the datum process to trigger a refresh."
  (let ((buf (or (and sql-datum--running-sqli-buf
                      (buffer-live-p sql-datum--running-sqli-buf)
                      sql-datum--running-sqli-buf)
                 (let ((b (sql-find-sqli-buffer 'datum)))
                   (and b (get-buffer b))))))
    (if (and buf (get-buffer-process buf))
        (comint-send-string (get-buffer-process buf) ":running\n")
      (message "datum: no active connection for refresh"))))

(defun sql-datum--running-start-timer ()
  "Start the auto-refresh timer for running queries."
  (sql-datum--running-stop-timer)
  (when sql-datum-running-refresh-interval
    (setq sql-datum--running-sqli-buf (sql-find-sqli-buffer 'datum))
    (setq sql-datum--running-timer
          (run-with-timer sql-datum-running-refresh-interval
                          sql-datum-running-refresh-interval
                          #'sql-datum--running-tick))))

(defun sql-datum--running-stop-timer ()
  "Stop the auto-refresh timer."
  (when sql-datum--running-timer
    (cancel-timer sql-datum--running-timer)
    (setq sql-datum--running-timer nil)))

(defun sql-datum--running-tick ()
  "Timer callback: refresh if the buffer is still visible, else stop."
  (if (get-buffer-window "*datum-running-queries*" t)
      (sql-datum--send-running)
    (sql-datum--running-stop-timer)))

;;; ---------------------------------------------------------------------------
;;; Goto Definition (M-.)
;;; ---------------------------------------------------------------------------

(defun sql-datum--identifier-at-point ()
  "Return the SQL identifier at point, including dotted and bracket-quoted names.
Strips bracket quoting from each part."
  (let ((start (point))
        (end (point))
        beg)
    (save-excursion
      ;; Move backward over identifier chars, dots, and brackets
      (skip-chars-backward "a-zA-Z0-9_.\\[\\]")
      (setq beg (point))
      ;; Move forward over identifier chars, dots, and brackets
      (goto-char start)
      (skip-chars-forward "a-zA-Z0-9_.\\[\\]")
      (setq end (point)))
    (when (> end beg)
      (let ((raw (buffer-substring-no-properties beg end)))
        ;; Strip bracket quoting from each dotted part
        (mapconcat (lambda (part)
                     (if (and (string-prefix-p "[" part)
                              (string-suffix-p "]" part))
                         (substring part 1 -1)
                       part))
                   (split-string raw "\\." t)
                   ".")))))

(defvar sql-datum--definition-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map "q" #'xref-go-back)
    map)
  "Keymap active in datum definition buffers.")

(define-minor-mode sql-datum--definition-mode
  "Minor mode for datum definition buffers.
Provides `q' to go back and `M-.' to dig deeper into definitions."
  :lighter " def"
  :keymap sql-datum--definition-mode-map)

(defun sql-datum--show-definition (object-name text &optional sqli-buf)
  "Display TEXT (DDL/source) for OBJECT-NAME in the current window.
Uses the xref marker stack so M-, navigates back.
SQLI-BUF, if given, is wired as the sql-buffer for send-region etc."
  (let* ((buf-name (format "*datum-def: %s*" object-name))
         (buf (get-buffer-create buf-name))
         (sqli (or sqli-buf
                   (let ((b (sql-find-sqli-buffer 'datum)))
                     (and b (get-buffer b))))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert text))
      (sql-mode)
      (sql-datum--definition-mode 1)
      (when sqli
        (setq-local sql-buffer sqli))
      (setq buffer-read-only t)
      (goto-char (point-min)))
    (switch-to-buffer buf)))

(defun sql-datum-goto-definition (name)
  "Look up the DDL/source of the SQL object NAME.
Pushes to the xref marker stack so M-, returns to the previous location.
With no identifier at point, prompts for a name."
  (interactive
   (let ((ident (sql-datum--identifier-at-point)))
     (list (if (and ident (not (string-empty-p ident)))
               ident
             (read-string "Definition of: ")))))
  (when (or (null name) (string-empty-p name))
    (user-error "No identifier provided"))
  (xref-push-marker-stack)
  (sql-datum--send-command (format ":definition %s" name)))

;;; ---------------------------------------------------------------------------
;;; Completion at point
;;; ---------------------------------------------------------------------------

(defun sql-datum--completion-match-p (prefix candidate)
  "Return non-nil if PREFIX matches CANDIDATE.
Matches against the full name, or if PREFIX has no dot, also against
the portion after the last dot (so \"Pat\" matches \"dbo.PatientDim\")."
  (or (string-prefix-p prefix candidate t)
      (and (not (string-match-p "\\." prefix))
           (string-match-p "\\." candidate)
           (string-prefix-p
            prefix
            (car (last (split-string candidate "\\.")))
            t))))

(defun sql-datum--make-completion-table (candidates _tables)
  "Build a completion table that also matches bare table name portions.
For a prefix without a dot, a candidate like \"rempat.fmreport\" matches
if the prefix matches either \"rempat.fmreport\" or \"fmreport\".
CANDIDATES is the full list."
  (lambda (string pred action)
    (pcase action
      ('metadata nil)
      ('t  ;; all-completions
       (let (result)
         (dolist (c candidates)
           (when (and (sql-datum--completion-match-p string c)
                      (or (null pred) (funcall pred c)))
             (push c result)))
         (nreverse result)))
      ('nil  ;; try-completion
       (let ((matches (funcall (sql-datum--make-completion-table
                                candidates nil)
                               string pred 't)))
         (cond ((null matches) nil)
               ((= (length matches) 1)
                (if (string= string (car matches)) t (car matches)))
               (t (try-completion "" matches)))))
      ('lambda  ;; test-completion
       (member string candidates))
      (_ nil))))

(defun sql-datum-completion-at-point ()
  "Provide SQL identifier completion using datum introspection data.
Automatically added to `completion-at-point-functions' in sql-mode
and sql-interactive-mode buffers that use datum."
  (when sql-datum-populate-completion
    (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode) (current-buffer))
                    (let ((b (sql-find-sqli-buffer 'datum)))
                      (and b (get-buffer b)))))
           (tables   (and buf (buffer-local-value 'sql-datum--tables   buf)))
           (schemas  (and buf (buffer-local-value 'sql-datum--schemas  buf)))
           (routines (and buf (buffer-local-value 'sql-datum--routines buf)))
           (col-hash (and buf (buffer-local-value 'sql-datum--columns  buf)))
           (columns  (when col-hash
                       (let (all)
                         (maphash (lambda (_k v)
                                    (setq all (append v all)))
                                  col-hash)
                         (delete-dups all))))
           (end     (point))
           (start   (save-excursion
                      (skip-chars-backward "a-zA-Z0-9_.#")
                      (point)))
           (candidates (append tables schemas routines columns)))
      (when candidates
        (list start end
              (sql-datum--make-completion-table candidates tables)
              :exclusive 'no
              :annotation-function
              (lambda (cand)
                (cond ((member cand tables)   " [table]")
                      ((member cand schemas)  " [schema]")
                      ((member cand routines) " [routine]")
                      ((member cand columns)  " [column]")
                      (t ""))))))))

(defun sql-datum--sql-mode-hook ()
  "Hook for `sql-mode' to enable datum completion and keybindings.
The capf function itself checks for an active datum connection and
returns nil if none is found, so this is safe for non-datum buffers."
  (add-hook 'completion-at-point-functions
            #'sql-datum-completion-at-point -90 t)
  (local-set-key (kbd "M-.") #'sql-datum-goto-definition))

(add-hook 'sql-mode-hook #'sql-datum--sql-mode-hook)

;;; ---------------------------------------------------------------------------
;;; Connection setup
;;; ---------------------------------------------------------------------------

(defvar sql-datum-login-params nil
  "This value is provided for compatibility with sql.el, do not change.")

(defvar sql-datum-options nil
  "This value is provided for compatibility with sql.el, do not change.")

(defun sql-comint-datum (product options &optional buf-name)
  "Create a comint buffer and connect to database using Datum.
PRODUCT is the sql product (datum).  OPTIONS are additional
parameters not defined in the customization.  BUF-NAME is the name
for the `comint' buffer."
  ;; Datum connects asynchronously via a background pyodbc call, so
  ;; sql.el's login delay is unnecessary and causes a visible freeze.
  ;; Bind it to 0 here so other SQL products are unaffected.
  (let ((sql-login-delay 0))
    (let ((parameters (append options
                              (unless (string-empty-p sql-server)
                                (list "--server" sql-server))
                              (unless (string-empty-p sql-database)
                                (list "--database" sql-database))
                              (sql-datum--comint-username)))
          (pass (sql-datum--comint-get-password)))
      (unless (and sql-connection parameters)
        (let ((conn-pair (sql-datum--prompt-connection)))
          (setf parameters (car conn-pair))
          (setf pass (cdr conn-pair))))
      (unless (or (null pass) (string-empty-p pass))
        (setf parameters
              (append parameters
                      (if sql-datum-password-variable
                          (progn
                            (setenv sql-datum-password-variable pass)
                            (list "--pass"
                                  (format "ENV=%s" sql-datum-password-variable)))
                        (list "--pass" pass)))))
      (sql-comint product parameters buf-name)
      (when sql-datum-password-variable
        (setenv sql-datum-password-variable))
      (sql-datum--setup-buffer (get-buffer (or buf-name "*SQL*"))))))

(defun sql-datum--setup-buffer (buf)
  "Set up BUF with the envelope filter and connection watcher."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      ;; Install the envelope filter — runs before output hits the buffer.
      (add-hook 'comint-preoutput-filter-functions
                #'sql-datum--preoutput-filter nil t)
      ;; Install completion-at-point in the SQLi buffer itself.
      ;; Prepend so it runs before comint's default filename completion.
      (add-hook 'completion-at-point-functions
                #'sql-datum-completion-at-point -90 t)
      ;; Watch for the first prompt to confirm connection.
      (letrec ((watcher
                (lambda (output)
                  (when (string-match-p (rx bol (* nonl) ">") output)
                    (message "datum: connected.")
                    (remove-hook 'comint-output-filter-functions watcher t)))))
        (add-hook 'comint-output-filter-functions watcher nil t))
      (message "datum: connecting in background..."))))

;;; ---------------------------------------------------------------------------
;;; Credential helpers (unchanged from original)
;;; ---------------------------------------------------------------------------

(defun sql-datum--comint-username ()
  "Determine the username for the connection."
  (if (eq 'auth-source sql-user)
      (list "--user" (plist-get (sql-datum--get-auth-source) :user))
    (unless (string-empty-p sql-user)
      (list "--user" sql-user))))

(defun sql-datum--comint-get-password ()
  "Determine the password for the connection."
  (if (eq 'auth-source sql-password)
      (auth-info-password (sql-datum--get-auth-source))
    (if (eq 'ask sql-password)
        (read-passwd "Password (empty to skip): ")
      (unless (string-empty-p sql-password)
        sql-password))))

(defun sql-datum--get-auth-source ()
  "Return the `auth-source' token for the current server@database pair."
  (require 'auth-source)
  (if-let ((auth-info (car (auth-source-search :host sql-connection
                                               :require '(:secret)))))
      auth-info
    (error "Didn't find the connection \"%s\" in auth-sources"
           sql-connection)))

(defun sql-datum--prompt-connection ()
  "Prompt for datum connection parameters interactively."
  (let ((parameters (if (y-or-n-p "Do you have a DSN? ")
                        (list "--dsn"
                              (read-string "DSN: "))
                      (list "--driver"
                            (read-string "ODBC Driver: "))))
        server database user password)
    (setf server (read-string "Server (empty to skip): "))
    (unless (string-empty-p server)
      (setf parameters (append parameters (list "--server" server))))
    (setf database (read-string "Database (empty to skip): "))
    (unless (string-empty-p database)
      (setf parameters (append parameters (list "--database" database))))
    (setf user (read-string "Username (empty to skip): "))
    (unless (string-empty-p user)
      (setf parameters (append parameters (list "--user" user))))
    (setf password (read-passwd "Password (empty to skip): "))
    (when (and (string-empty-p user) (string-empty-p password))
      (when (y-or-n-p "No user nor password provided.  Use Integrated security? ")
        (setf parameters (append parameters (list "--integrated")))))
    (when (y-or-n-p "Specify a config file? ")
      (setf parameters (append parameters (list "--config"
                                                (read-file-name "Config file path: ")))))
    (cons parameters password)))

;;; ---------------------------------------------------------------------------
;;; Interactive commands
;;; ---------------------------------------------------------------------------

;;;###autoload
(defun sql-datum (&optional buffer)
  "Run Datum as an inferior process.
The buffer with name BUFFER will be used or created."
  (interactive "P")
  (when (or (symbolp sql-user) (null sql-user))
    (setf sql-user ""))
  (when (or (symbolp sql-password) (null sql-password))
    (setf sql-password ""))
  (sql-product-interactive 'datum buffer))

(defun sql-datum-copy-last-result ()
  "Copy the last query result block from the datum buffer to the kill ring."
  (interactive)
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (unless buf
      (user-error "No active datum buffer found"))
    (with-current-buffer buf
      (save-excursion
        (goto-char (point-max))
        ;; Search back for the separator line (dashes) that precedes result rows
        (if (re-search-backward "^-[-\s]+" nil t)
            (let* ((result-start (line-beginning-position))
                   (result-end   (progn
                                   (re-search-forward "^Rows affected:" nil t)
                                   (line-end-position))))
              (kill-ring-save result-start result-end)
              (message "datum: last result copied to kill ring (%d chars)"
                       (- result-end result-start)))
          (user-error "datum: no result found in buffer"))))))

(defun sql-datum-complete-table ()
  "Insert a table name from the introspection cache using `completing-read'."
  (interactive)
  (let* ((buf     (sql-find-sqli-buffer 'datum))
         (tables  (and buf (buffer-local-value 'sql-datum--tables buf))))
    (if tables
        (insert (completing-read "Table: " tables nil t))
      (user-error "datum: no tables cached — run :tables first"))))

(defun sql-datum-export ()
  "Export query results to a file via :out.

Without a prefix argument, prompts for a table name and exports
the entire table (SELECT * FROM table).

With a prefix argument (\\[universal-argument]), prompts for a SQL query
instead, allowing arbitrary queries to be exported.

In both cases, prompts for a file path (format inferred from
extension: .csv, .parquet, .json).  If the file exists, asks for
confirmation before overwriting."
  (interactive)
  (let* ((use-query current-prefix-arg)
         (query (if use-query
                    (read-string "SQL query: " "SELECT * FROM ")
                  (let ((tbl (sql-datum--read-table "Export table: ")))
                    (format "SELECT * FROM %s" tbl))))
         (file (read-file-name "Export to: " nil nil nil nil
                               (lambda (f)
                                 (or (file-directory-p f)
                                     (string-match-p "\\.\\(csv\\|parquet\\|json\\)\\'" f)))))
         (abs-path (expand-file-name file))
         (force (when (file-exists-p abs-path)
                  (y-or-n-p (format "%s exists. Overwrite? " abs-path))))
         (force-flag (if force " :force" ""))
         (buf (sql-find-sqli-buffer 'datum)))
    (when (and (file-exists-p abs-path) (not force))
      (user-error "Export cancelled"))
    (unless buf
      (user-error "No active datum buffer found"))
    (let ((proc (get-buffer buf)))
      (comint-send-string proc (format ":out %s%s\n" abs-path force-flag))
      (comint-send-string proc (format "%s;;\n" query)))
    (message "datum: exporting to %s%s"
             abs-path (if force " (overwrite)" ""))))

(defun sql-datum-import (path table-name mode batch-size)
  "Import file at PATH into TABLE-NAME via :in.
Prompts for a file path, table name (with completion from cache),
and import mode.  BATCH-SIZE controls rows per executemany call;
defaults to `sql-datum-import-batch-size', overridden with \\[universal-argument]."
  (interactive
   (let* ((file (read-file-name "Import file: " nil nil t nil
                                (lambda (f)
                                  (or (file-directory-p f)
                                      (string-match-p "\\.\\(csv\\|parquet\\|json\\)\\'" f)))))
          (buf  (sql-find-sqli-buffer 'datum))
          (tables (and buf (buffer-local-value 'sql-datum--tables
                                               (get-buffer buf))))
          (table (completing-read "Into table: " tables nil nil))
          (mode  (completing-read "Mode: "
                                  '("default (error if exists)"
                                    ":insert (append)"
                                    ":replace (drop & recreate)")
                                  nil t nil nil
                                  "default (error if exists)"))
          (batch (if current-prefix-arg
                     (read-number "Batch size: " sql-datum-import-batch-size)
                   sql-datum-import-batch-size)))
     (list file table
           (pcase mode
             ((pred (string-prefix-p ":insert"))  ":insert")
             ((pred (string-prefix-p ":replace")) ":replace")
             (_ nil))
           batch)))
  (let* ((abs-path (expand-file-name path))
         (mode-flag (if mode (concat " " mode) ""))
         (batch-flag (format " :batch %d" batch-size))
         (buf (sql-find-sqli-buffer 'datum)))
    (unless buf
      (user-error "No active datum buffer found"))
    (comint-send-string (get-buffer buf)
                        (format ":in %s %s%s%s\n"
                                abs-path table-name mode-flag batch-flag))
    (message "datum: importing %s into %s%s (batch size %d)"
             (file-name-nondirectory abs-path) table-name
             (or mode-flag " (default)") batch-size)))

(defun sql-datum--send-command (cmd)
  "Send CMD string to the active datum process.
If the SQLi buffer is not currently visible, display it."
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (unless buf
      (user-error "No active datum buffer found"))
    (let ((buf-obj (get-buffer buf)))
      (unless (get-buffer-window buf-obj t)
        (display-buffer buf-obj))
      (comint-send-string buf-obj (concat cmd "\n")))))

(defun sql-datum--get-dialect ()
  "Return the SQL dialect string from the active datum SQLi buffer."
  (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode) (current-buffer))
                  (let ((b (sql-find-sqli-buffer 'datum)))
                    (and b (get-buffer b))))))
    (and buf (buffer-local-value 'sql-datum--dialect buf))))

(defun sql-datum--read-table (prompt)
  "Read a table name with completion from the introspection cache.
PROMPT is displayed to the user."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (tables (and buf (buffer-local-value 'sql-datum--tables
                                              (get-buffer buf)))))
    (completing-read prompt tables nil nil)))

(defun sql-datum--mssql-p ()
  "Return non-nil if the current dialect is MSSQL."
  (string= (sql-datum--get-dialect) "mssql"))

(defun sql-datum-top (n)
  "Select the top N rows from a table (default 10).
Uses TOP syntax for MSSQL, LIMIT for others."
  (interactive "P")
  (let* ((count (or (and n (prefix-numeric-value n)) 10))
         (table (sql-datum--read-table "Top rows from table: "))
         (sql (if (sql-datum--mssql-p)
                  (format "SELECT TOP %d * FROM %s;;" count table)
                (format "SELECT * FROM %s LIMIT %d;;" table count))))
    (sql-datum--send-command sql)))

(defun sql-datum-count (table)
  "Select COUNT(*) from TABLE.
Uses COUNT_BIG on MSSQL to handle tables with more than 2^31 rows."
  (interactive (list (sql-datum--read-table "Count rows in table: ")))
  (let ((func (if (sql-datum--mssql-p) "COUNT_BIG" "COUNT")))
    (sql-datum--send-command (format "SELECT %s(*) FROM %s;;" func table))))

(defun sql-datum-sample (n)
  "Select a random sample of N rows from a table (default 10).
Uses NEWID() for MSSQL, RANDOM() for others."
  (interactive "P")
  (let* ((count (or (and n (prefix-numeric-value n)) 10))
         (table (sql-datum--read-table "Sample rows from table: "))
         (sql (if (sql-datum--mssql-p)
                  (format "SELECT TOP %d * FROM %s ORDER BY NEWID();;" count table)
                (format "SELECT * FROM %s ORDER BY RANDOM() LIMIT %d;;" table count))))
    (sql-datum--send-command sql)))

(defalias 'sql-datum-describe #'sql-datum-columns
  "Alias for `sql-datum-columns'.")

(defun sql-datum-table-exists (table)
  "Check if TABLE exists in the database.
Uses OBJECT_ID for MSSQL, to_regclass for others."
  (interactive (list (sql-datum--read-table "Check existence of table: ")))
  (let ((sql (if (sql-datum--mssql-p)
                 (format "SELECT OBJECT_ID('%s');;" table)
               (format "SELECT to_regclass('%s');;" table))))
    (sql-datum--send-command sql)))

(defun sql-datum-drop-table (table)
  "Drop TABLE after confirmation.
Prompts for a table name with completion from the introspection cache,
then asks for explicit confirmation before sending DROP TABLE."
  (interactive (list (sql-datum--read-table "Drop table: ")))
  (when (or (not sql-datum-confirm-drop)
            (y-or-n-p (format "Drop table %s? " table)))
    (sql-datum--send-command (format "DROP TABLE %s;;" table))
    (message "datum: dropped %s" table)))

(defun sql-datum-pwd ()
  "Show current user, server, database, and version via :pwd."
  (interactive)
  (sql-datum--send-command ":pwd"))

(defun sql-datum-tables (filter)
  "List all tables and views via :tables.
With a prefix argument, prompt for a filter pattern."
  (interactive (list (when current-prefix-arg
                       (read-string "Filter tables: "))))
  (sql-datum--send-command (if filter
                               (format ":tables %s" filter)
                             ":tables")))

(defun sql-datum-columns (table)
  "List columns for TABLE via :columns."
  (interactive (list (sql-datum--read-table "Columns for table: ")))
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (database (and buf (gethash "database"
                                     (buffer-local-value 'sql-datum--meta
                                                         (get-buffer buf))
                                     ""))))
    (sql-datum--send-command (format ":columns %s" table))
    (message "datum: columns for %s%s"
             table
             (if (string-empty-p database) ""
               (format " (database: %s)" database)))))

(defun sql-datum-databases (filter)
  "List all databases via :databases.
With a prefix argument, prompt for a filter pattern."
  (interactive (list (when current-prefix-arg
                       (read-string "Filter databases: "))))
  (sql-datum--send-command (if filter
                               (format ":databases %s" filter)
                             ":databases")))

(defun sql-datum-schemas (filter)
  "List all schemas via :schemas.
With a prefix argument, prompt for a filter pattern."
  (interactive (list (when current-prefix-arg
                       (read-string "Filter schemas: "))))
  (sql-datum--send-command (if filter
                               (format ":schemas %s" filter)
                             ":schemas")))

(defun sql-datum-routines (filter)
  "List stored procedures and functions via :routines.
With a prefix argument, prompt for a filter pattern."
  (interactive (list (when current-prefix-arg
                       (read-string "Filter routines: "))))
  (sql-datum--send-command (if filter
                               (format ":routines %s" filter)
                             ":routines")))

(defun sql-datum-running ()
  "List currently running queries via :running."
  (interactive)
  (sql-datum--send-command ":running"))

(defun sql-datum-version ()
  "Show server version via :version."
  (interactive)
  (sql-datum--send-command ":version"))

(defun sql-datum-user ()
  "Show current database user via :user."
  (interactive)
  (sql-datum--send-command ":user"))

(defun sql-datum-use-database (db)
  "Switch to database DB via :use.
Prompts with completion from the cached database list."
  (interactive
   (let* ((buf (sql-find-sqli-buffer 'datum))
          (databases (and buf (buffer-local-value 'sql-datum--databases
                                                  (get-buffer buf)))))
     (list (completing-read "Switch to database: " databases nil nil))))
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (when buf
      (with-current-buffer (get-buffer buf)
        (letrec ((watcher
                  (lambda (output)
                    (when (string-match-p (rx bol (* nonl) ">") output)
                      (message "datum: connected to %s." db)
                      (remove-hook 'comint-output-filter-functions watcher t)))))
          (add-hook 'comint-output-filter-functions watcher nil t)))))
  (sql-datum--send-command (format ":use %s" db))
  (message "datum: switching to database %s (reconnecting...)" db))

;;;###autoload
(defun sql-datum-scratch (&optional new)
  "Open a scratch SQL buffer associated with the active datum connection.
If a *datum-scratch* buffer already exists, just switch to it.
With prefix argument NEW, create an additional scratch buffer with
a numeric suffix, prompting to confirm the name."
  (interactive "P")
  (let* ((base "*datum-scratch*")
         (name (if new
                   (let* ((n 2)
                          (candidate (format "*datum-scratch-%d*" n)))
                     (while (get-buffer candidate)
                       (setq n (1+ n))
                       (setq candidate (format "*datum-scratch-%d*" n)))
                     (read-string "Buffer name: " candidate))
                 base))
         (buf (get-buffer-create name))
         (sqli (sql-find-sqli-buffer 'datum)))
    (switch-to-buffer buf)
    (unless (derived-mode-p 'sql-mode)
      (sql-mode))
    (when sqli
      (setq-local sql-buffer sqli))
    (message "datum: scratch buffer ready — C-c C-r to send region, C-c C-b to send buffer")))

;;; ---------------------------------------------------------------------------
;;; Keybindings
;;; ---------------------------------------------------------------------------

(with-eval-after-load 'sql
  ;; C-c t: table operations
  (define-key sql-mode-map (kbd "C-c t e") #'sql-datum-export)
  (define-key sql-mode-map (kbd "C-c t i") #'sql-datum-import)
  (define-key sql-mode-map (kbd "C-c t t") #'sql-datum-top)
  (define-key sql-mode-map (kbd "C-c t c") #'sql-datum-count)
  (define-key sql-mode-map (kbd "C-c t s") #'sql-datum-sample)
  (define-key sql-mode-map (kbd "C-c t d") #'sql-datum-describe)
  (define-key sql-mode-map (kbd "C-c t x") #'sql-datum-table-exists)
  (define-key sql-mode-map (kbd "C-c t D") #'sql-datum-drop-table)
  ;; C-c s: session info
  (define-key sql-mode-map (kbd "C-c s p") #'sql-datum-pwd)
  (define-key sql-mode-map (kbd "C-c s t") #'sql-datum-tables)
  (define-key sql-mode-map (kbd "C-c s c") #'sql-datum-columns)
  (define-key sql-mode-map (kbd "C-c s d") #'sql-datum-databases)
  (define-key sql-mode-map (kbd "C-c s s") #'sql-datum-schemas)
  (define-key sql-mode-map (kbd "C-c s R") #'sql-datum-routines)
  (define-key sql-mode-map (kbd "C-c s r") #'sql-datum-running)
  (define-key sql-mode-map (kbd "C-c s v") #'sql-datum-version)
  (define-key sql-mode-map (kbd "C-c s u") #'sql-datum-user)
  ;; C-c u: switch database
  (define-key sql-mode-map (kbd "C-c u")   #'sql-datum-use-database)
)

;;; ---------------------------------------------------------------------------
;;; Product registration
;;; ---------------------------------------------------------------------------

(unless (assoc 'datum sql-product-alist)
  (sql-add-product 'datum "Datum - ODBC Client"
                   :free-software t
                   :prompt-regexp "^.*>"
                   :prompt-cont-regexp "^.*>"
                   :sqli-comint-func 'sql-comint-datum
                   :sqli-login 'sql-datum-login-params
                   :sqli-program 'sql-datum-program
                   :sqli-options 'sql-datum-options))

(provide 'sql-datum)
;;; sql-datum.el ends here
