;;; sql-datum.el --- Emacs SQL IDE via Datum and ODBC   -*- lexical-binding: t; -*-

;; Copyright (C) 2021-2024 Sebastian Monia
;; Copyright (C) 2026 Gary Hollis
;;
;; Author: Sebastian Monia <smonia@outlook.com>
;; Author: Gary Hollis <ghollisjr@gmail.com>
;; URL: https://github.com/ghollisjr/datum
;; Package-Requires: ((emacs "28.1"))
;; Version: 2.0
;; Keywords: languages processes tools

;; This file is not part of GNU Emacs.

;;; SPDX-License-Identifier: MIT

;;; Commentary:

;; Emacs SQL IDE built on sql.el and the Datum Python CLI.  Provides
;; completion-at-point for tables, columns, schemas, and routines (with
;; parameter signatures), M-. goto-definition with xref stack navigation,
;; eldoc, one-keystroke table operations, import/export, and more.
;;
;; Setup:
;;   1. Place sql-datum.el in your load-path.
;;   2. (require 'sql-datum)
;; Then...
;;   3. M-x sql-datum will prompt for parameters to create a connection
;; - OR -
;;   3. Add to sql-connection-alist an item that uses Datum to connect,
;;      it will show up in the candidates when calling sql-connect.
;;
;; For a detailed user manual and additional setup examples, see:
;; https://github.com/ghollisjr/datum

;;; Code:

(require 'sql)
(require 'cl-lib)
(require 'xref)

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

(defcustom sql-datum-auto-introspect t
  "When non-nil, automatically run :refresh after connecting.
This populates autocomplete for tables, schemas, databases, and
routines without needing to manually run introspection commands."
  :type 'boolean
  :group 'SQL)

(defcustom sql-datum-refresh-interval nil
  "Seconds between automatic introspection refreshes.
When non-nil, a timer periodically sends :refresh to keep
completion candidates up to date.  Set to nil to disable."
  :type '(choice (const :tag "Disabled" nil) integer)
  :group 'SQL)

(defcustom sql-datum-prefer-ansi-quotes nil
  "When non-nil, prefer ANSI double-quote quoting for all dialects.
When nil (the default), use dialect-specific quoting: backticks
for MySQL, square brackets for MSSQL."
  :type 'boolean
  :group 'SQL)

(defvar sql-datum--refresh-timer nil
  "Timer for periodic introspection refresh.")


;;; ---------------------------------------------------------------------------
;;; Buffer-local state
;;; ---------------------------------------------------------------------------

(defvar-local sql-datum--dialect nil
  "Detected SQL dialect for this datum buffer (e.g. \"mssql\", \"postgres\").")

(defvar-local sql-datum--default-schema nil
  "Default schema for this connection (e.g. \"dbo\", \"public\").
Nil for databases without schemas.  Set from the default-schema
meta envelope at connect time.")

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
  "Hash table mapping downcased table name to list of column name strings.
Keys are always downcased, optionally schema-qualified: \"users\" or
\"dbo.users\".  Always downcase lookup keys before calling gethash.
The consistent keying is load-bearing — do not reintroduce maphash
scans here without first breaking the normalization invariant.")

(defvar-local sql-datum--column-details (make-hash-table :test #'equal)
  "Hash table mapping downcased table name to full column metadata rows.
Each value is a list of [col_name type nullable default] vectors.
Keys follow the same downcased convention as `sql-datum--columns'.")

(defvar-local sql-datum--columns-pending (make-hash-table :test #'equal)
  "Hash table tracking in-flight silent column fetches.
Keys are downcased table names, values are t.  Prevents duplicate fetch requests.")

(defvar-local sql-datum--routine-signatures (make-hash-table :test #'equal)
  "Hash table mapping downcased routine name to parameter signature string.
Keys are always downcased for consistent lookup.
Populated by the routine-sigs introspect envelope.")

(defvar-local sql-datum--routine-types (make-hash-table :test #'equal)
  "Hash table mapping downcased routine name to type string.
Values are \"FUNCTION\" or \"PROCEDURE\".  Keys are always downcased
for consistent lookup.  Populated by the routine-types introspect envelope.")

(defvar-local sql-datum--xdb-cache (make-hash-table :test #'equal)
  "Cross-database completion cache for MSSQL.
Hash: database name -> plist with keys :tables :schemas :routines
:routine-types :routine-sigs :pending.")

(defvar-local sql-datum--ready nil
  "Non-nil when the Python process is idle and ready for a command.
Set by the `ready' envelope, cleared when a command is sent.")

(defvar sql-datum--trace-enabled nil
  "When non-nil, log diagnostic messages to `*datum-trace*' buffer.")

(defun sql-datum--trace (fmt &rest args)
  "When tracing is enabled, log a timestamped message to `*datum-trace*'."
  (when sql-datum--trace-enabled
    (let ((msg (apply #'format fmt args))
          (ts (format-time-string "%H:%M:%S.%3N")))
      (with-current-buffer (get-buffer-create "*datum-trace*")
        (goto-char (point-max))
        (insert (format "[%s] %s\n" ts msg))))))


(defvar-local sql-datum--suppress-prompt-count 0
  "Number of upcoming prompts to suppress in the preoutput filter.
Incremented by silent commands (e.g. M-. definition, background refresh).")

(defvar-local sql-datum--prompt-suppressed nil
  "Set to t by the preoutput filter when a prompt was just suppressed.
Used internally by the preoutput filter's prompt-suppression logic.
Readiness detection now uses `sql-datum--ready' (set by the `ready'
envelope from Python) rather than this flag.")

(defvar-local sql-datum--refresh-in-progress nil
  "Non-nil while an async refresh chain is running.
Prevents overlapping refresh chains.")

;;; ---------------------------------------------------------------------------
;;; Command queue
;;; ---------------------------------------------------------------------------

(defvar-local sql-datum--command-queue nil
  "FIFO queue of pending command transactions.
Each element is a plist with keys :commands :silent :setup-fn :done-fn :priority.")

(defvar-local sql-datum--queue-current nil
  "The currently in-flight transaction plist, or nil.")

(defvar-local sql-datum--queue-remaining nil
  "Remaining command strings in the current in-flight transaction.")

(defun sql-datum--enqueue (transaction)
  "Append TRANSACTION plist to the command queue and pump.
TRANSACTION is a plist with keys :commands :silent :setup-fn :done-fn :priority.
:priority defaults to :normal.  :low priority transactions are inserted
after all :normal ones."
  (let ((priority (or (plist-get transaction :priority) :normal)))
    (if (eq priority :low)
        ;; Insert after last :normal transaction
        (let ((pos 0)
              (q sql-datum--command-queue))
          (while (and q (eq (or (plist-get (car q) :priority) :normal) :normal))
            (setq pos (1+ pos)
                  q (cdr q)))
          (if (= pos (length sql-datum--command-queue))
              (setq sql-datum--command-queue
                    (append sql-datum--command-queue (list transaction)))
            (let ((before (seq-take sql-datum--command-queue pos))
                  (after (seq-drop sql-datum--command-queue pos)))
              (setq sql-datum--command-queue
                    (append before (list transaction) after)))))
      (setq sql-datum--command-queue
            (append sql-datum--command-queue (list transaction)))))
  (sql-datum--queue-pump))

(defun sql-datum--enqueue-one (cmd &rest args)
  "Enqueue a single-command transaction for CMD.
ARGS are keyword args: :silent :done-fn :priority."
  (let ((silent (plist-get args :silent))
        (done-fn (plist-get args :done-fn))
        (priority (plist-get args :priority)))
    (sql-datum--enqueue
     (list :commands (list cmd)
           :silent silent
           :done-fn done-fn
           :priority (or priority :normal)))))

(defun sql-datum--queue-pump ()
  "If no transaction is in-flight and the queue is non-empty, start the next one."
  (when (and (null sql-datum--queue-current)
             sql-datum--command-queue
             sql-datum--ready)
    (let ((txn (pop sql-datum--command-queue)))
      (setq sql-datum--queue-current txn
            sql-datum--queue-remaining (plist-get txn :commands))
      (when-let ((setup (plist-get txn :setup-fn)))
        (funcall setup))
      (sql-datum--queue-send-next))))

(defun sql-datum--queue-send-next ()
  "Send the next command in the current in-flight transaction."
  (when (and sql-datum--queue-current sql-datum--queue-remaining)
    (let ((cmd (pop sql-datum--queue-remaining))
          (proc (get-buffer-process (current-buffer))))
      (when proc
        (setq sql-datum--ready nil)
        (when (plist-get sql-datum--queue-current :silent)
          (cl-incf sql-datum--suppress-prompt-count))
        (comint-send-string proc (concat cmd "\n"))))))

(defun sql-datum--queue-advance ()
  "Called when a `ready' envelope arrives.  Advance the queue state."
  (cond
   ;; Current transaction has more commands — send next
   (sql-datum--queue-remaining
    (sql-datum--queue-send-next))
   ;; Current transaction is done — call done-fn and pump next
   (sql-datum--queue-current
    (let ((done-fn (plist-get sql-datum--queue-current :done-fn)))
      (setq sql-datum--queue-current nil
            sql-datum--queue-remaining nil)
      (when done-fn (funcall done-fn))
      (sql-datum--queue-pump)))
   ;; Nothing in-flight — pump in case something queued while busy
   (t
    (sql-datum--queue-pump))))

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
    (let ((result (string-join (nreverse clean-lines) "\n")))
      ;; When silent commands are pending, suppress the prompt and any
      ;; echoed command text.  The pty echoes the command back (e.g.
      ;; ":refresh-tables\r\n"), and Python emits "\n>" as the next
      ;; prompt.  After envelope stripping these appear as residual text
      ;; ending with ">".  We strip the trailing prompt when present and
      ;; suppress all remaining text (echo/whitespace from silent cmd).
      (setq sql-datum--prompt-suppressed nil)
      (when (> sql-datum--suppress-prompt-count 0)
        (cond
         ;; Result ends with the ">" prompt — suppress everything
         ;; (the echoed command, whitespace, and the prompt itself).
         ((string-match-p "[\n\r ]*>\\'" result)
          (setq result "")
          (cl-decf sql-datum--suppress-prompt-count)
          (setq sql-datum--prompt-suppressed t))
         ;; Prompt hasn't arrived yet (output was split across chunks).
         ;; Suppress the echoed command text and whitespace so they
         ;; don't leak into the buffer.  We know this output belongs
         ;; to a silent command because suppress-prompt-count > 0.
         ((string-match-p "\\`[^>]*\\'" result)
          (setq result ""))))
      result)))

(defvar sql-datum--running-timer nil
  "Timer for auto-refreshing the running queries buffer.")

(defvar sql-datum--running-sqli-buf nil
  "The SQLi buffer used for running-queries auto-refresh.")

(defvar sql-datum--running-quit-flag nil
  "Non-nil when the user has explicitly quit the running buffer.")

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
         (sql-datum--update-mode-line)
         (when (equal key "default-schema")
           (setq sql-datum--default-schema val))
         ;; Database changed — clear stale completion state and refresh
         (when (equal key "database")
           (setq sql-datum--schemas nil)
           (setq sql-datum--tables nil)
           (setq sql-datum--routines nil)
           (clrhash sql-datum--columns)
           (clrhash sql-datum--column-details)
           (clrhash sql-datum--columns-pending)
           (clrhash sql-datum--routine-signatures)
           (clrhash sql-datum--routine-types)
           (sql-datum--refresh-async (current-buffer))))))
    ("result-file"
     ;; Greedy .* captures path (may contain colons, e.g. C:\path),
     ;; [^:]+ captures the format suffix after the last colon.
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
       ;; If the user quit the buffer, ignore stale responses
       (if sql-datum--running-quit-flag
           (setq sql-datum--running-quit-flag nil)
         ;; Remember which SQLi buffer sent this, for refresh later
         (setq sql-datum--running-sqli-buf (current-buffer))
         (let ((initial (not sql-datum--running-timer)))
           (sql-datum--show-running-queries text initial)
           (when initial
             (sql-datum--running-start-timer))))))
    ("ready"
     (sql-datum--trace "READY envelope received, setting sql-datum--ready=t")
     (setq sql-datum--ready t)
     (sql-datum--queue-advance))
    ("definition"
     ;; Payload is JSON: {"name": ..., "text": ...}
     (let* ((parsed (json-parse-string payload :object-type 'alist))
            (obj-name (alist-get 'name parsed))
            (text (replace-regexp-in-string "\\\\n" "\n"
                                            (alist-get 'text parsed)))
            (sqli-buf (current-buffer)))
       ;; Defer to avoid disrupting comint's process filter context
       (run-at-time 0 nil #'sql-datum--show-definition
                    obj-name text sqli-buf)))))

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
          ("routine-sigs"
           (clrhash sql-datum--routine-signatures)
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair)
                        sql-datum--routine-signatures))))
          ("routine-types"
           (clrhash sql-datum--routine-types)
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair)
                        sql-datum--routine-types))))
          ((pred (string-prefix-p "columns:"))
           (let* ((raw-table (substring kind (length "columns:")))
                  (canon-key (downcase raw-table)))
             ;; Items are now full rows [name, type, nullable, default].
             ;; Store just names for completion, full rows for metadata.
             (puthash canon-key
                      (mapcar (lambda (row) (if (consp row) (car row) row))
                              items)
                      sql-datum--columns)
             (puthash canon-key items sql-datum--column-details)))
          ((pred (string-prefix-p "xdb:"))
           (sql-datum--handle-xdb-introspect kind items))))
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
          ("routine-sigs"
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair)
                        sql-datum--routine-signatures))))
          ("routine-types"
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair)
                        sql-datum--routine-types))))
          ((pred (string-prefix-p "columns:"))
           (let* ((raw-table (substring kind (length "columns:")))
                  (canon-key (downcase raw-table))
                  (existing-names (gethash canon-key sql-datum--columns))
                  (existing-details (gethash canon-key sql-datum--column-details))
                  (new-names (mapcar (lambda (row) (if (consp row) (car row) row))
                                     items)))
             (puthash canon-key (append existing-names new-names) sql-datum--columns)
             (puthash canon-key (append existing-details items) sql-datum--column-details)))
          ((pred (string-prefix-p "xdb:"))
           (sql-datum--handle-xdb-introspect kind items t))))
    (error (message "datum: failed to parse introspect+ payload: %s" err))))

(defun sql-datum--handle-xdb-introspect (kind items &optional append)
  "Route an xdb:<db>:<subkind> introspect envelope into `sql-datum--xdb-cache'.
KIND is the full \"xdb:dbname:subkind\" string; ITEMS is the parsed payload.
When APPEND is non-nil, append to existing lists instead of replacing."
  (when (string-match "^xdb:\\([^:]+\\):\\(.*\\)$" kind)
    (let* ((db (match-string 1 kind))
           (subkind (match-string 2 kind))
           (entry (or (gethash db sql-datum--xdb-cache)
                      (list :pending nil))))
      (pcase subkind
        ("schemas"
         (setq entry (plist-put entry :schemas
                                (if append
                                    (append (plist-get entry :schemas) items)
                                  items))))
        ("tables"
         (setq entry (plist-put entry :tables
                                (if append
                                    (append (plist-get entry :tables) items)
                                  items))))
        ("routines"
         (setq entry (plist-put entry :routines
                                (if append
                                    (append (plist-get entry :routines) items)
                                  items))))
        ("routine-types"
         (let ((ht (or (plist-get entry :routine-types)
                       (make-hash-table :test #'equal))))
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair) ht)))
           (setq entry (plist-put entry :routine-types ht))))
        ("routine-sigs"
         (let ((ht (or (plist-get entry :routine-sigs)
                       (make-hash-table :test #'equal))))
           (dolist (pair items)
             (when (and (consp pair) (>= (length pair) 2))
               (puthash (downcase (nth 0 pair)) (nth 1 pair) ht)))
           (setq entry (plist-put entry :routine-sigs ht))))
        ("done"
         (setq entry (plist-put entry :pending nil))))
      (puthash db entry sql-datum--xdb-cache))))

(defcustom sql-datum-running-refresh-interval 5
  "Seconds between auto-refresh of the running queries buffer.
Set to nil to disable auto-refresh."
  :type '(choice (const :tag "Disabled" nil) integer)
  :group 'SQL)

(defun sql-datum--show-running-queries (text &optional display)
  "Display TEXT in a dedicated running-queries buffer.
TEXT is pre-formatted tabular output from the Python printer.
When DISPLAY is non-nil, pop up the buffer; otherwise just update it."
  (let ((buf (get-buffer-create "*datum-running-queries*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (format "datum: Running Queries & Jobs  (last refresh: %s)"
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
    (when display
      (display-buffer buf))))

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
  (setq sql-datum--running-quit-flag t)
  (quit-window t))

(defun sql-datum--send-running ()
  "Send :running to the datum process to trigger a refresh."
  (let ((buf (or (and sql-datum--running-sqli-buf
                      (buffer-live-p sql-datum--running-sqli-buf)
                      sql-datum--running-sqli-buf)
                 (let ((b (sql-find-sqli-buffer 'datum)))
                   (and b (get-buffer b))))))
    (if (and buf (get-buffer-process buf))
        (with-current-buffer buf
          (sql-datum--enqueue-one ":running" :silent t :priority :low))
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
  "Timer callback: refresh the running buffer (even if not visible)."
  (if (get-buffer "*datum-running-queries*")
      (sql-datum--send-running)
    (sql-datum--running-stop-timer)))

;;; ---------------------------------------------------------------------------
;;; Identifier quoting helpers
;;; ---------------------------------------------------------------------------

(defconst sql-datum--quote-styles
  '((:open ?\"  :close ?\"  :dialect nil)           ; ANSI — all dialects
    (:open ?\[  :close ?\]  :dialect ("mssql"))
    (:open ?\`  :close ?\`  :dialect ("mysql")))
  "SQL identifier quote styles.
:dialect nil means valid in all dialects.
:dialect (list) means valid only in those dialects.")

(defun sql-datum--active-quote-styles (dialect)
  "Return quote styles active for DIALECT.
nil dialect returns all styles.  When `sql-datum-prefer-ansi-quotes'
is nil, dialect-specific styles are sorted first so that quoting
functions prefer them over ANSI double-quotes."
  (let ((active (cl-remove-if-not
                 (lambda (s)
                   (let ((d (plist-get s :dialect)))
                     (or (null d) (null dialect) (member dialect d))))
                 sql-datum--quote-styles)))
    (if sql-datum-prefer-ansi-quotes
        active
      ;; Put dialect-specific styles first, ANSI (nil dialect) last.
      (append (cl-remove-if-not (lambda (s) (plist-get s :dialect)) active)
              (cl-remove-if     (lambda (s) (plist-get s :dialect)) active)))))

(defun sql-datum--unquote-part (part)
  "Strip bracket, double-quote, or backtick quoting from a single identifier PART.
Handles both closed quotes and unclosed leading quotes (mid-typing).
\"[foo bar]\" → \"foo bar\", \"\\\"foo\\\"\" → \"foo\", \"`foo`\" → \"foo\"."
  (let ((len (length part)))
    (cl-loop for s in sql-datum--quote-styles
             for open  = (string (plist-get s :open))
             for close = (string (plist-get s :close))
             if (and (string-prefix-p open part)
                     (string-suffix-p close part)
                     (or (not (string= open close)) (> len 1)))
             return (substring part 1 -1)
             else if (string-prefix-p open part)
             return (substring part 1)
             finally return part)))

(defun sql-datum--split-identifier (raw)
  "Split RAW identifier string on dots, respecting bracket, double-quote, and backtick quoting.
Returns a list of parts (still quoted).  E.g.
  \"public.\\\"my table\\\"\" → (\"public\" \"\\\"my table\\\"\")
  \"dbo.[my col]\"       → (\"dbo\" \"[my col]\")."
  (let ((parts nil)
        (current "")
        (in-quote nil)  ; holds close-char when inside quotes, nil otherwise
        (i 0)
        (len (length raw)))
    (while (< i len)
      (let ((ch (aref raw i)))
        (cond
         ;; Inside a quoted region — check for closing char
         ((and in-quote (eq ch in-quote))
          (setq in-quote nil)
          (setq current (concat current (string ch))))
         (in-quote
          (setq current (concat current (string ch))))
         ;; Outside quotes — check for opening chars
         ((cl-loop for s in sql-datum--quote-styles
                   when (eq ch (plist-get s :open))
                   do (setq in-quote (plist-get s :close))
                      (setq current (concat current (string ch)))
                   and return t)
          ;; handled by cl-loop side effects
          )
         ;; Dot separator
         ((eq ch ?.)
          (unless (string-empty-p current)
            (push current parts))
          (setq current ""))
         ;; Normal character
         (t
          (setq current (concat current (string ch))))))
      (setq i (1+ i)))
    (unless (string-empty-p current)
      (push current parts))
    (nreverse parts)))

(defun sql-datum--needs-quoting-p (name)
  "Return non-nil if identifier NAME contains characters requiring SQL quoting."
  (and (stringp name)
       (not (string-empty-p name))
       (string-match-p "[^a-zA-Z0-9_.#@]" name)))

(defun sql-datum--quote-segment (segment dialect)
  "Quote a single identifier SEGMENT for DIALECT if it needs quoting.
Uses the first matching style from `sql-datum--quote-styles' for DIALECT."
  (if (sql-datum--needs-quoting-p segment)
      (let ((style (car (sql-datum--active-quote-styles dialect))))
        (concat (string (plist-get style :open))
                segment
                (string (plist-get style :close))))
    segment))

(defun sql-datum--quote-identifier (name dialect)
  "Quote each dotted segment of NAME that needs quoting for DIALECT.
E.g. \"public.my table\" → \"public.\\\"my table\\\"\" for postgres,
or \"dbo.my col\" → \"dbo.[my col]\" for mssql."
  (if (not (sql-datum--needs-quoting-p name))
      name
    (mapconcat (lambda (seg) (sql-datum--quote-segment seg dialect))
               (split-string name "\\." t)
               ".")))

(defun sql-datum--maybe-quote-completed (cand start dialect)
  "After completion inserts CAND at START, replace with quoted form if needed.
DIALECT determines quoting style.  If the user already typed an
opening quote before START, it is consumed and the user's chosen
quote style is preserved (double-quotes are ANSI-valid everywhere).
CAND may already be quoted (from Python introspection); it is
unquoted first to avoid double-quoting."
  (let ((real-start start)
        (has-leading-quote nil)
        (user-quote-style nil)
        ;; Unquote first so already-quoted candidates don't get re-quoted.
        (bare (sql-datum--unquote-identifier cand)))
    ;; Check for a user-typed opening quote/bracket/backtick just before start.
    ;; Double-quote is ANSI and valid in every dialect.
    (when (> real-start (point-min))
      (let ((prev-char (char-after (1- real-start))))
        (when (cl-some (lambda (s)
                         (and (eq prev-char (plist-get s :open))
                              (let ((d (plist-get s :dialect)))
                                (or (null d) (member dialect d)))))
                       sql-datum--quote-styles)
          (setq real-start (1- real-start))
          (setq has-leading-quote t)
          (setq user-quote-style prev-char))))
    ;; Use the user's chosen quote style if they started one,
    ;; otherwise fall back to dialect default.
    (let* ((effective-dialect
            (if user-quote-style
                (let ((match (cl-find-if
                              (lambda (s) (eq user-quote-style (plist-get s :open)))
                              sql-datum--quote-styles)))
                  (or (car (plist-get match :dialect)) dialect))
              dialect))
           (quoted (sql-datum--quote-identifier bare effective-dialect)))
      (when (or has-leading-quote (not (string= cand quoted)))
        (let ((end (point)))
          (delete-region real-start end)
          (goto-char real-start)
          (insert quoted))))))

;;; ---------------------------------------------------------------------------
;;; Goto Definition (M-.)
;;; ---------------------------------------------------------------------------

(defun sql-datum--scan-quoted-identifier (direction &optional dialect)
  "Scan in DIRECTION (-1 backward, 1 forward) over a possibly-quoted SQL identifier.
Handles bare identifiers and dialect-appropriate quoting.
DIALECT controls which quote styles are recognized; nil means all.
Returns the new position."
  (let ((styles (sql-datum--active-quote-styles dialect)))
    ;; If point is inside a quoted region, jump to the boundary first so
    ;; the main loop sees the quote character and handles the segment.
    (dolist (s styles)
      (let* ((open-str  (string (plist-get s :open)))
             (close-str (string (plist-get s :close)))
             (qopen (save-excursion
                      (search-backward open-str (line-beginning-position) t)))
             (qclose (when qopen
                       (save-excursion
                         (goto-char (1+ qopen))
                         (search-forward close-str (line-end-position) t)))))
        (when (and qopen qclose
                   (< qopen (point))
                   (>= qclose (point)))
          (if (eq direction -1)
              (goto-char qopen)
            (goto-char qclose)))))
    ;; Main scanning loop
    (let ((keep-going t))
      (while keep-going
        (setq keep-going nil)
        (let ((handled nil))
          ;; Try each active quote style
          (cl-loop
           for s in styles
           for open-ch  = (plist-get s :open)
           for close-ch = (plist-get s :close)
           for same-char = (eq open-ch close-ch)
           do
           (cond
            ;; Forward into opening quote
            ((and (not handled) (eq direction 1) (eq (char-after) open-ch))
             (forward-char)
             (search-forward (string close-ch) nil t)
             (setq keep-going (eq (char-after) ?.))
             (setq handled t))
            ;; Backward from closing quote — same-char styles need
            ;; the between-check to avoid jumping past other identifiers.
            ((and (not handled) (eq direction -1) (eq (char-before) close-ch) same-char)
             (let ((quote-pos (1- (point))))
               (backward-char)
               (let ((found (search-backward (string open-ch)
                                             (line-beginning-position) t)))
                 (if (and found
                          (not (string-match-p
                                (regexp-quote (string open-ch))
                                (buffer-substring-no-properties
                                 (1+ found) quote-pos))))
                     (setq keep-going (eq (char-before) ?.))
                   ;; No valid match — treat as unclosed opening quote
                   (goto-char quote-pos)
                   (setq keep-going (eq (char-before) ?.)))))
             (setq handled t))
            ;; Backward from closing quote — different-char styles (e.g. [])
            ((and (not handled) (eq direction -1) (eq (char-before) close-ch) (not same-char))
             (backward-char)
             (search-backward (string open-ch) nil t)
             (setq keep-going (eq (char-before) ?.))
             (setq handled t))))
          ;; Bare identifier segment
          (unless handled
            (if (eq direction 1)
                (skip-chars-forward "a-zA-Z0-9_#@$")
              (skip-chars-backward "a-zA-Z0-9_#@$"))
            (setq keep-going
                  (if (eq direction 1)
                      (eq (char-after) ?.)
                    (eq (char-before) ?.)))))
        ;; Skip the dot to continue to the next segment
        (when keep-going
          (forward-char direction)))
      (point))))

(defun sql-datum--identifier-at-point-raw ()
  "Return the raw SQL identifier at point, preserving quoting.
Dotted segments that are double-quoted, bracket-quoted, or
backtick-quoted are kept as-is so the backend can distinguish
quoted from unquoted names.  Uses the current dialect to decide
which quote characters are valid."
  (let ((dialect (sql-datum--get-dialect))
        beg end)
    (save-excursion
      (setq beg (sql-datum--scan-quoted-identifier -1 dialect))
      (setq end (sql-datum--scan-quoted-identifier 1 dialect)))
    (when (> end beg)
      (buffer-substring-no-properties beg end))))

(defun sql-datum--identifier-at-point ()
  "Return the SQL identifier at point, including dotted and quoted names.
Handles bracket quoting ([name]) and double-quote quoting (\"name\"),
returning the bare (unquoted) identifier for lookup."
  (let ((raw (sql-datum--identifier-at-point-raw)))
    (when raw
      (let ((parts (sql-datum--split-identifier raw)))
        (mapconcat #'sql-datum--unquote-part parts ".")))))

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
   (let ((ident (sql-datum--identifier-at-point-raw)))
     (list (if (and ident (not (string-empty-p ident)))
               ident
             (read-string "Definition of: ")))))
  (when (or (null name) (string-empty-p name))
    (user-error "No identifier provided"))
  (xref-push-marker-stack)
  (sql-datum--send-command (format ":definition %s" name) t))

;;; ---------------------------------------------------------------------------
;;; Completion at point
;;; ---------------------------------------------------------------------------

(defun sql-datum--strip-leading-quotes (s &optional dialect)
  "Strip a leading quote character from S appropriate for DIALECT.
When DIALECT is nil, strip any known quote."
  (if (and (> (length s) 0)
           (cl-some (lambda (st)
                      (eq (aref s 0) (plist-get st :open)))
                    (sql-datum--active-quote-styles dialect)))
      (substring s 1)
    s))

(defun sql-datum--make-hash-set (list)
  "Return a hash table mapping each element of LIST to t."
  (let ((h (make-hash-table :test #'equal :size (length list))))
    (dolist (x list) (puthash x t h))
    h))

(defun sql-datum--unquote-identifier (name)
  "Unquote all segments of a dotted SQL identifier NAME.
\"public.\\\"test table\\\"\" → \"public.test table\"."
  (mapconcat #'sql-datum--unquote-part
             (sql-datum--split-identifier name) "."))

(defun sql-datum--completion-match-p (prefix candidate &optional ds-prefix)
  "Return non-nil if PREFIX matches CANDIDATE.
Compares unquoted forms so that a typed prefix like public.\"test
matches a candidate like public.\"test table\".
Matches against the full name, or if PREFIX has no dot, also against
the portion after the last dot (so \"Pat\" matches \"dbo.PatientDim\").
When DS-PREFIX is non-nil, skip the after-dot fallback for candidates
that start with DS-PREFIX — their bare forms are already in the list."
  (let ((bare-prefix (sql-datum--unquote-identifier prefix))
        (bare-candidate (sql-datum--unquote-identifier candidate)))
    (or (string-prefix-p prefix candidate t)
        (string-prefix-p bare-prefix bare-candidate t)
        (and (not (string-match-p "\\." prefix))
             (string-match-p "\\." candidate)
             ;; Skip after-dot match when candidate is in the default
             ;; schema — the bare form is already a separate candidate.
             (not (and ds-prefix (string-prefix-p ds-prefix candidate t)))
             (let ((after-dot (car (last (sql-datum--split-identifier
                                          candidate)))))
               (or (string-prefix-p prefix after-dot t)
                   (string-prefix-p
                    bare-prefix
                    (sql-datum--unquote-part after-dot)
                    t)))))))

(defun sql-datum--make-completion-table (candidates &optional sort-fn ds-prefix)
  "Build a completion table that also matches bare table name portions.
For a prefix without a dot, a candidate like \"rempat.fmreport\" matches
if the prefix matches either \"rempat.fmreport\" or \"fmreport\".
CANDIDATES is the full list.  SORT-FN, when non-nil, is used as
the `display-sort-function' in metadata.  DS-PREFIX, when non-nil,
is the default schema prefix (e.g. \"public.\") — candidates starting
with it are excluded from after-dot fallback matching since their
bare forms are already in CANDIDATES."
  (lambda (string pred action)
    (pcase action
      ('metadata
       (if sort-fn
           `(metadata (category . sql-datum-identifier)
                      (display-sort-function . ,sort-fn)
                      (cycle-sort-function . ,sort-fn))
         '(metadata (category . sql-datum-identifier))))
      ('t  ;; all-completions
       (let (result)
         (dolist (c candidates)
           (when (and (sql-datum--completion-match-p string c ds-prefix)
                      (or (null pred) (funcall pred c)))
             (push c result)))
         (nreverse result)))
      ('nil  ;; try-completion
       (let ((matches (funcall (sql-datum--make-completion-table
                                candidates nil ds-prefix)
                               string pred 't)))
         (cond ((null matches) nil)
               ((= (length matches) 1)
                (if (string= (sql-datum--strip-leading-quotes string)
                              (car matches))
                    t
                  (car matches)))
               (t (try-completion "" matches)))))
      ('lambda  ;; test-completion
       (member (sql-datum--strip-leading-quotes string) candidates))
      (_ nil))))

(defun sql-datum--parse-param-names (sig)
  "Extract parameter names from a signature string.
E.g. \"@Table_name varchar(128), @Other int\" → (\"@Table_name\" \"@Other\")."
  (when (and sig (not (string-empty-p sig)))
    (let (names (parts (split-string sig ",")))
      (dolist (part parts)
        (let* ((trimmed (string-trim part))
               (name (car (split-string trimmed "[ \t]+"))))
          (when (and name (not (string-empty-p name)))
            (push name names))))
      (nreverse names))))

(defun sql-datum--sigs-canonical-key (name sigs)
  "Return the canonical key in SIGS that matches NAME, or nil.
Keys in SIGS are downcased.  Tries exact match first, then bare name
against qualified keys, then strips the schema prefix from a qualified
NAME to match a bare key."
  (when (and name (stringp name))
    (let ((name (downcase name)))
    (cond
     ((gethash name sigs) name)
   ;; Bare name → check if any schema.name key matches
   ((not (string-match-p "\\." name))
    (let (found)
      (maphash (lambda (k _v)
                 (when (and (not found)
                            (string-match-p "\\." k)
                            (string= name
                                     (car (last (split-string k "\\.")))))
                   (setq found k)))
               sigs)
      found))
   ;; Qualified name → try schema.name suffix, then bare name
   (t
    (let* ((parts (split-string name "\\."))
           ;; For 3+-part names, try the last two segments (schema.name)
           (schema-name (when (>= (length parts) 3)
                          (mapconcat #'identity (last parts 2) ".")))
           (bare (car (last parts))))
      (or (and schema-name (gethash schema-name sigs) schema-name)
          (when (gethash bare sigs) bare))))))))

(defun sql-datum--at-new-param-p (routine-name sigs)
  "Return non-nil if point is where a new @parameter name is expected.
ROUTINE-NAME is the canonical sigs key.  SIGS is the signatures hash.
Checks that we are not inside a string literal, not in a value position
\(after =), and are either right after the routine name, after a comma,
or typing an @-prefixed word."
  (and
   ;; Not inside a string literal
   (not (nth 3 (syntax-ppss)))
   (let* ((prefix-start (save-excursion
                           (skip-chars-backward "a-zA-Z0-9_.@#")
                           (point)))
          (prefix (buffer-substring-no-properties prefix-start (point))))
     (or
      ;; Already typing an @param
      (string-prefix-p "@" prefix)
      ;; Empty prefix — check what's before us
      (and (string-empty-p prefix)
           (save-excursion
             (skip-chars-backward " \t")
             (let ((ch (char-before)))
               (or
                ;; After a comma → next param position
                (eq ch ?,)
                ;; Right after the routine name itself (flexible match)
                (let ((ident (sql-datum--identifier-at-point)))
                  (and ident
                       (string= (or (sql-datum--sigs-canonical-key ident sigs) "")
                                routine-name)))))))))))

(defun sql-datum--find-param-routine (sigs)
  "Find the enclosing routine for parameter completion context.
Returns the routine name string if point is in a parameter position,
or nil otherwise.  Checks three cases:
  1. Inside parentheses of a function call.
  2. Right after a routine name with only whitespace between.
  3. On the same statement line as a procedure call (routine at start)."
  (when (and sigs (> (hash-table-count sigs) 0))
    (or
     ;; Case 1: inside parentheses — look before the open paren
     (save-excursion
       (let ((paren-pos (nth 1 (syntax-ppss))))
         (when paren-pos
           (goto-char paren-pos)
           (skip-chars-backward " \t")
           (let ((name (sql-datum--identifier-at-point)))
             (sql-datum--sigs-canonical-key name sigs)))))
     ;; Case 2: right after routine name + whitespace
     (save-excursion
       (skip-chars-backward " \t")
       (let ((name (sql-datum--identifier-at-point)))
         (sql-datum--sigs-canonical-key name sigs)))
     ;; Case 3: further along on same statement — routine must be
     ;; the first identifier (after optional EXEC/EXECUTE)
     (save-excursion
       (let* ((stmt-start (save-excursion
                            (or (and (search-backward ";"
                                                      (line-beginning-position) t)
                                     (1+ (point)))
                                (if (derived-mode-p 'sql-interactive-mode)
                                    (comint-line-beginning-position)
                                  (line-beginning-position)))))
              name)
         (goto-char stmt-start)
         (skip-chars-forward " \t")
         ;; Skip EXEC/EXECUTE keyword if present
         (when (looking-at "\\(?:EXEC\\(?:UTE\\)?\\)\\b[ \t]*")
           (goto-char (match-end 0)))
         ;; The next identifier should be the routine name
         (setq name (sql-datum--identifier-at-point))
         (sql-datum--sigs-canonical-key name sigs))))))

(defun sql-datum--routine-uses-parens-p (routine-name rtypes dialect)
  "Return non-nil if ROUTINE-NAME should use parenthesized call syntax.
Functions always use parens.  On PostgreSQL, procedures also use
parens (CALL proc(args)), unlike MSSQL which uses EXEC proc @p=val."
  (let ((rtype (and rtypes (gethash (downcase routine-name) rtypes))))
    (or (equal rtype "FUNCTION")
        (and (equal rtype "PROCEDURE")
             (equal dialect "postgres")))))

(defun sql-datum--statement-bounds ()
  "Return (START . END) for the SQL statement around point.
In sql-interactive-mode, the statement is bounded by semicolons or
the current input line.  In other modes (scratch buffers), the
statement is bounded by semicolons or the current paragraph (blank
lines), matching `sql-datum-send-smart' behavior."
  (let ((fallback-start (if (derived-mode-p 'sql-interactive-mode)
                            (comint-line-beginning-position)
                          (save-excursion (backward-paragraph) (point))))
        (fallback-end (if (derived-mode-p 'sql-interactive-mode)
                          (point-max)
                        (save-excursion (forward-paragraph) (point)))))
    (cons (save-excursion
            (or (and (search-backward ";" fallback-start t)
                     (1+ (point)))
                fallback-start))
          (save-excursion
            (or (and (search-forward ";" fallback-end t)
                     (1- (point)))
                fallback-end)))))

(defun sql-datum--tables-in-statement ()
  "Return list of table names referenced in the current SQL statement.
Scans both before and after point for FROM, JOIN, UPDATE, INTO keywords
and collects the table identifiers that follow them.  Handles
comma-separated table lists (e.g. FROM t1, t2).  Returns nil if no
tables are found."
  (save-excursion
    (let* ((dialect (sql-datum--get-dialect))
           (bounds (sql-datum--statement-bounds))
           (_trace-bounds (sql-datum--trace "tables-in-statement: bounds=(%d . %d) text=|%s|"
                                            (car bounds) (cdr bounds)
                                            (buffer-substring-no-properties
                                             (car bounds) (cdr bounds))))
           (stmt-start (car bounds))
           (stmt-end (cdr bounds))
           (table-kw-re (concat "\\<\\("
                                "FROM\\|"
                                "\\(?:LEFT\\|RIGHT\\|INNER\\|FULL\\|CROSS\\|OUTER\\|NATURAL\\)?[ \t]*JOIN\\|"
                                "UPDATE\\|"
                                "INTO"
                                "\\)\\>"))
           (stop-kw-re (concat "\\<\\("
                               "WHERE\\|ON\\|SET\\|ORDER\\|GROUP\\|HAVING\\|"
                               "LIMIT\\|UNION\\|VALUES\\|SELECT\\|"
                               "LEFT\\|RIGHT\\|INNER\\|FULL\\|CROSS\\|OUTER\\|NATURAL\\|"
                               "JOIN\\|FROM\\|INTO\\|UPDATE"
                               "\\)\\>"))
           (tables nil))
      (goto-char stmt-start)
      (while (re-search-forward table-kw-re stmt-end t)
        (skip-chars-forward " \t\n")
        ;; Collect table names, handling comma-separated lists
        (let ((continue t))
          (while (and continue (<= (point) stmt-end))
            (skip-chars-forward " \t\n")
            (when (>= (point) stmt-end)
              (setq continue nil))
            (when continue
              ;; Check if we hit a clause-terminating keyword
              (if (looking-at stop-kw-re)
                  (setq continue nil)
                ;; Read a table identifier (may be quoted)
                (let ((id-start (point)))
                  (sql-datum--scan-quoted-identifier 1 dialect)
                  (if (= (point) id-start)
                      (setq continue nil)  ; no identifier found
                    (let* ((raw (buffer-substring-no-properties id-start (point)))
                           (cleaned (mapconcat
                                     #'sql-datum--unquote-part
                                     (sql-datum--split-identifier raw)
                                     ".")))
                      (unless (string-empty-p cleaned)
                        (push cleaned tables)))
                    ;; Check for comma to continue list
                    (skip-chars-forward " \t\n")
                    (if (and (< (point) stmt-end) (eq (char-after) ?,))
                        (forward-char 1)
                      (setq continue nil)))))))))
      (delete-dups (nreverse tables)))))

(defun sql-datum--table-aliases-in-statement ()
  "Return an alist of (NAME . TABLE) for tables in the current statement.
Each table gets an entry mapping its own name to itself.  If an alias
follows the table name (e.g. FROM orders o, JOIN users AS u), an
additional entry maps the alias to the table."
  (save-excursion
    (let* ((dialect (sql-datum--get-dialect))
           (bounds (sql-datum--statement-bounds))
           (stmt-start (car bounds))
           (stmt-end (cdr bounds))
           (table-kw-re (concat "\\<\\("
                                "FROM\\|"
                                "\\(?:LEFT\\|RIGHT\\|INNER\\|FULL\\|CROSS\\|OUTER\\|NATURAL\\)?[ \t]*JOIN\\|"
                                "UPDATE\\|"
                                "INTO"
                                "\\)\\>"))
           (stop-kw-re (concat "\\<\\("
                                "WHERE\\|ON\\|SET\\|ORDER\\|GROUP\\|HAVING\\|"
                                "LIMIT\\|UNION\\|VALUES\\|SELECT\\|"
                                "LEFT\\|RIGHT\\|INNER\\|FULL\\|CROSS\\|OUTER\\|NATURAL\\|"
                                "JOIN\\|FROM\\|INTO\\|UPDATE"
                                "\\)\\>"))
           (alias-kw-re "\\<AS\\>")
           (result nil))
      (goto-char stmt-start)
      (while (re-search-forward table-kw-re stmt-end t)
        (skip-chars-forward " \t\n")
        (let ((continue t))
          (while (and continue (<= (point) stmt-end))
            (skip-chars-forward " \t\n")
            (when (>= (point) stmt-end)
              (setq continue nil))
            (when continue
              (if (looking-at stop-kw-re)
                  (setq continue nil)
                (let ((id-start (point)))
                  (sql-datum--scan-quoted-identifier 1 dialect)
                  (if (= (point) id-start)
                      (setq continue nil)
                    (let* ((raw (buffer-substring-no-properties id-start (point)))
                           (cleaned (mapconcat
                                     #'sql-datum--unquote-part
                                     (sql-datum--split-identifier raw)
                                     ".")))
                      (unless (string-empty-p cleaned)
                        (push (cons cleaned cleaned) result)
                        ;; Look for alias: optional AS keyword then identifier
                        (skip-chars-forward " \t\n")
                        (when (looking-at alias-kw-re)
                          (goto-char (match-end 0))
                          (skip-chars-forward " \t\n"))
                        ;; Next token might be an alias (if not a keyword or comma)
                        (unless (or (looking-at stop-kw-re)
                                    (looking-at ",")
                                    (>= (point) stmt-end))
                          (let ((alias-start (point)))
                            (sql-datum--scan-quoted-identifier 1 dialect)
                            (unless (= (point) alias-start)
                              (let* ((alias-raw (buffer-substring-no-properties
                                                 alias-start (point)))
                                     (alias (mapconcat
                                             #'sql-datum--unquote-part
                                             (sql-datum--split-identifier alias-raw)
                                             ".")))
                                (unless (or (string-empty-p alias)
                                            (string-equal-ignore-case alias "ON")
                                            (string-equal-ignore-case alias "WHERE")
                                            (string-equal-ignore-case alias "SET")
                                            (string-equal-ignore-case alias "JOIN")
                                            (string-equal-ignore-case alias "LEFT")
                                            (string-equal-ignore-case alias "RIGHT")
                                            (string-equal-ignore-case alias "INNER")
                                            (string-equal-ignore-case alias "FULL")
                                            (string-equal-ignore-case alias "CROSS")
                                            (string-equal-ignore-case alias "OUTER")
                                            (string-equal-ignore-case alias "NATURAL"))
                                  (push (cons alias cleaned) result))))))))
                    ;; Check for comma to continue list
                    (skip-chars-forward " \t\n")
                    (if (and (< (point) stmt-end) (eq (char-after) ?,))
                        (forward-char 1)
                      (setq continue nil)))))))))
      (nreverse result))))

(defun sql-datum--fetch-columns-async (table buf col-hash pending-hash)
  "Fetch columns for TABLE silently in the background.
BUF is the SQLi process buffer.  COL-HASH is `sql-datum--columns',
PENDING-HASH is `sql-datum--columns-pending'.
Skips if TABLE is already cached or already in-flight.

Used for background prefetching (e.g. inside dynamic completion
tables).  For completion-time fetches, prefer the synchronous
`sql-datum--fetch-columns-sync' which blocks briefly with timeout."
  ;; All cache keys are downcased — normalize the lookup key.
  (let ((key (downcase table)))
    (sql-datum--trace "fetch-columns-async: table=%s key=%s cached=%s pending=%s ready=%s"
                      table key
                      (if (gethash key col-hash) "yes" "no")
                      (gethash key pending-hash)
                      (buffer-local-value 'sql-datum--ready buf))
    (unless (or (gethash key col-hash)
                (gethash key pending-hash))
      (let ((proc (get-buffer-process buf)))
        (when proc
          ;; The queue handles ordering — just enqueue the fetch.
          (sql-datum--fetch-columns-send table key buf proc pending-hash))))))

(defvar sql-datum-column-fetch-timeout 2.0
  "Maximum seconds to wait for column data during completion.
If columns don't arrive within this time, completion proceeds
without them and a warning is shown.")

(defun sql-datum--explicit-completion-p ()
  "Return non-nil if completion was explicitly triggered by the user.
Returns nil during company idle completion (automatic as-you-type),
where blocking with `accept-process-output' would freeze Emacs."
  (or (not (bound-and-true-p company-mode))
      (bound-and-true-p company--manual-action)))

(defun sql-datum--fetch-columns-sync (tables buf)
  "Fetch columns for TABLES synchronously, waiting until all arrive or timeout.
BUF is the SQLi process buffer.  Sends :columns TABLE :silent for each
uncached table, then waits for process output until all columns are in
the cache or `sql-datum-column-fetch-timeout' expires.
Returns t if all columns are cached, nil if timed out."
  (let* ((proc (get-buffer-process buf))
         (col-hash (buffer-local-value 'sql-datum--columns buf))
         (pending-hash (buffer-local-value 'sql-datum--columns-pending buf))
         (needed nil))
    ;; Cannot safely send direct commands while a queued transaction is in-flight.
    (when (and proc col-hash pending-hash
              (not (buffer-local-value 'sql-datum--queue-current buf)))
      ;; Identify which tables need fetching.
      (dolist (tbl tables)
        (let ((key (downcase tbl)))
          (unless (gethash key col-hash)
            (push key needed)
            ;; Send the command if not already in-flight.
            (unless (gethash key pending-hash)
              (sql-datum--trace "fetch-columns-sync: SENDING :columns %s :silent" tbl)
              (with-current-buffer buf
                (puthash key t pending-hash)
                (setq sql-datum--ready nil)
                (cl-incf sql-datum--suppress-prompt-count)
                (comint-send-string proc (format ":columns %s :silent\n" tbl)))))))
      (if (null needed)
          t
        ;; Wait for all columns to arrive.  Use a sentinel to detect
        ;; empty responses (gethash returns nil for both "absent" and
        ;; "mapped to nil").
        (sql-datum--trace "fetch-columns-sync: waiting for %s" needed)
        (let ((deadline (+ (float-time) sql-datum-column-fetch-timeout))
              (sentinel (make-symbol "missing")))
          (while (and needed (< (float-time) deadline))
            (accept-process-output proc 0.05)
            (setq needed (cl-remove-if
                          (lambda (key)
                            (not (eq (gethash key col-hash sentinel)
                                     sentinel)))
                          needed)))
          ;; Clean up pending-hash for keys that arrived.
          (dolist (tbl tables)
            (let ((key (downcase tbl)))
              (when (not (eq (gethash key col-hash sentinel) sentinel))
                (remhash key pending-hash))))
          (if needed
              (progn
                (sql-datum--trace "fetch-columns-sync: TIMEOUT waiting for %s" needed)
                (message "datum: timed out waiting for column data for: %s"
                         (mapconcat #'identity needed ", "))
                nil)
            (sql-datum--trace "fetch-columns-sync: all columns cached")
            t))))))

(defun sql-datum--fetch-columns-send (table key buf proc pending-hash)
  "Send the :columns command for TABLE to PROC.
KEY is the downcased cache key.  BUF is the SQLi buffer.
PENDING-HASH tracks in-flight requests.  PROC is kept for API compat."
  (ignore proc)
  (sql-datum--trace "fetch-columns-send: SENDING :columns %s :silent" table)
  (with-current-buffer buf
    (puthash key t pending-hash)
    (sql-datum--enqueue-one (format ":columns %s :silent" table)
                            :silent t :priority :low)))

(defun sql-datum--xdb-fetch-async (db buf xdb-cache callback)
  "Send :refresh-db DB asynchronously.  Call CALLBACK when cache entry is ready.
BUF is the SQLi process buffer, XDB-CACHE the cross-db hash."
  (ignore xdb-cache)
  (let ((proc (get-buffer-process buf)))
    (when proc
      (message "datum: fetching objects for %s..." db)
      (with-current-buffer buf
        (puthash db (list :pending t) sql-datum--xdb-cache)
        (sql-datum--enqueue-one (format ":refresh-db %s" db)
                                :silent t :priority :low
                                :done-fn (lambda ()
                                           (message "datum: %s ready" db)
                                           (when callback (funcall callback))))))))

(defun sql-datum--member-ignore-case (elt list)
  "Like `member' but uses case-insensitive comparison.
Returns the tail of LIST whose car matches ELT, or nil."
  (let ((result nil))
    (while (and list (not result))
      (when (string-equal-ignore-case elt (car list))
        (setq result list))
      (setq list (cdr list)))
    result))

(defun sql-datum--xdb-completion (prefix start end buf dbs xdb-cache)
  "Handle cross-database completion for MSSQL.
PREFIX is the typed text, START/END delimit it.  BUF is the SQLi
buffer.  DBS is the database list, XDB-CACHE the cross-db hash.
Returns a completion spec or nil if PREFIX is not a known database."
  (let* ((first-dot (string-match "\\." prefix))
         (db-part (and first-dot (substring prefix 0 first-dot)))
         ;; Use canonical (server-side) casing for the database name
         (db-match (and db-part
                        (car (sql-datum--member-ignore-case
                              db-part dbs)))))
    (when db-match
      ;; Fetch asynchronously if not cached yet
      (unless (gethash db-match xdb-cache)
        (sql-datum--xdb-fetch-async db-match buf xdb-cache nil))
      (let ((entry (gethash db-match xdb-cache)))
        ;; If still pending, tell the user to TAB again
        (when (plist-get entry :pending)
          (message "datum: fetching %s, TAB again shortly" db-match))
        ;; Return a completion spec with a *dynamic* table that reads
        ;; from xdb-cache on every query.  This is critical because
        ;; Emacs caches the completion table returned by the capf —
        ;; if we close over a snapshot of candidates taken while the
        ;; fetch is still pending, the table stays empty forever.
        (let ((db-key db-match)
              (cache xdb-cache))
          (list start end
                (lambda (string pred action)
                  (let* ((cur (gethash db-key cache))
                         (tables   (plist-get cur :tables))
                         (routines (plist-get cur :routines))
                         (db-pfx   (concat db-key "."))
                         (cands (let (filtered)
                                  (dolist (c (append tables routines))
                                    (when (string-prefix-p db-pfx c t)
                                      (push c filtered)))
                                  (nreverse filtered))))
                    (funcall
                     (sql-datum--make-completion-table cands)
                     string pred action)))
                :exclusive t
                :annotation-function
                (lambda (cand)
                  (let* ((cur (gethash db-match xdb-cache))
                         (tables   (plist-get cur :tables))
                         (routines (plist-get cur :routines)))
                    (cond ((member cand tables)   " [table]")
                          ((member cand routines) " [routine]")
                          (t ""))))
                :exit-function
                (lambda (cand status)
                  (when (eq status 'finished)
                    ;; MSSQL always uses brackets
                    (sql-datum--maybe-quote-completed cand start "mssql")
                    (let* ((cur (gethash db-match xdb-cache))
                           (xrtypes (plist-get cur :routine-types))
                           (xsigs   (plist-get cur :routine-sigs))
                           (dc (downcase cand)))
                      (when (and xrtypes
                                 (equal (gethash dc xrtypes) "FUNCTION"))
                        (insert "()")
                        (backward-char)
                        (when (and xsigs (gethash dc xsigs))
                          (message "%s(%s)" cand
                                   (gethash dc xsigs)))))))))))))


(defun sql-datum-completion-at-point ()
  "Provide SQL identifier completion using datum introspection data.
Automatically added to `completion-at-point-functions' in sql-mode
and sql-interactive-mode buffers that use datum.

When point is in a parameter context (inside function parens, right
after a routine name, or on the same line as a procedure call),
offers parameter name completion instead of normal identifiers.
Completing a FUNCTION name auto-inserts parentheses."
  (sql-datum--trace "capf: called, populate=%s sql-buffer=%s"
                    sql-datum-populate-completion
                    (if (boundp 'sql-buffer) sql-buffer "unbound"))
  (when (and sql-datum-populate-completion
             ;; Suppress inside single-quoted strings only.  Double quotes
             ;; are SQL identifier quotes and should still complete.
             (not (eq (nth 3 (syntax-ppss)) ?')))
    (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode) (current-buffer))
                    (let ((b (sql-find-sqli-buffer 'datum)))
                      (and b (get-buffer b)))))
           (sigs     (and buf (buffer-local-value 'sql-datum--routine-signatures buf)))
           (rtypes   (and buf (buffer-local-value 'sql-datum--routine-types buf)))
           (col-hash (and buf (buffer-local-value 'sql-datum--columns  buf)))
           (dialect (and buf (buffer-local-value 'sql-datum--dialect buf)))
           (routine-ctx (sql-datum--find-param-routine sigs))
           (in-parens (nth 1 (syntax-ppss))))
      (cond
       ;; --- Parameter context: right after a routine that uses parens ---
       ;; Insert () and place cursor inside.
       ;; Functions always use parens; PostgreSQL procedures also use parens.
       ((and routine-ctx (not in-parens)
             (save-excursion
               (skip-chars-backward " \t")
               (let ((ident (sql-datum--identifier-at-point)))
                 (and ident
                      (string= (or (sql-datum--sigs-canonical-key ident sigs) "")
                               routine-ctx))))
             (sql-datum--routine-uses-parens-p routine-ctx rtypes dialect))
        (let ((start (save-excursion
                       (skip-chars-backward " \t")
                       (point)))
              (end (point)))
          (list start end '("()")
                :exclusive 'no
                :exit-function (lambda (_cand _status)
                                 (backward-char)
                                 (when-let ((msg (sql-datum-eldoc-function)))
                                   (message "%s" msg))))))
       ;; --- Parameter context: procedure params (not inside parens-style call) ---
       ;; For MSSQL procedures: offer @param name completion.
       ;; Skip if inside parens of a routine that uses parens syntax.
       ((and routine-ctx
             (not (and in-parens
                       (sql-datum--routine-uses-parens-p routine-ctx rtypes dialect)))
             (sql-datum--at-new-param-p routine-ctx sigs))
        (let* ((sig (gethash routine-ctx sigs))
               (params (sql-datum--parse-param-names sig))
               (end (point))
               (start (save-excursion
                        (skip-chars-backward "a-zA-Z0-9_.@#")
                        (point))))
          (when params
            (list start end params
                  :exclusive 'no
                  :annotation-function
                  (lambda (_) (format " [param of %s]" routine-ctx))))))
       ;; --- Normal identifier completion ---
       (t
        (let* ((end (save-excursion
                      (sql-datum--scan-quoted-identifier 1 dialect)
                      (point)))
               (start (save-excursion
                        (sql-datum--scan-quoted-identifier -1 dialect)
                        (point)))
               (raw-prefix (buffer-substring-no-properties start end))
               ;; Strip quotes from prefix so it matches bare candidates
               (dialect (and buf (buffer-local-value 'sql-datum--dialect buf)))
               (dbs     (and buf (buffer-local-value 'sql-datum--databases buf)))
               (xdb-cache (and buf (buffer-local-value 'sql-datum--xdb-cache buf)))
               ;; Check for cross-database prefix (mssql only).
               ;; Use raw-prefix so trailing dots are preserved
               ;; (e.g. "CDW_NEW." keeps the dot for db-part extraction).
               (xdb-result (when (and (equal dialect "mssql")
                                      (string-match-p "\\." raw-prefix)
                                      dbs)
                             (sql-datum--xdb-completion
                              raw-prefix start end buf dbs xdb-cache))))
          (or xdb-result
              ;; --- Qualified column completion (table.col or alias.col) ---
              ;; Only enter this path when the prefix ends with a dot
              ;; (e.g. "users." or "dbo.users.") indicating the user is
              ;; starting a column name.  A dot in the middle of a
              ;; schema-qualified table (e.g. "dbo.users") must NOT trigger
              ;; column completion — that is a normal identifier.
              (when (and (not xdb-result)
                         (string-suffix-p "." raw-prefix)
                         col-hash)
                (let* ((parts (sql-datum--split-identifier raw-prefix))
                       ;; For "dbo.users." parts is ["dbo" "users" ""],
                       ;; so non-empty becomes ["dbo" "users"] → tbl-part.
                       (non-empty (cl-remove-if #'string-empty-p parts))
                       (tbl-part (mapconcat #'sql-datum--unquote-part non-empty "."))
                       (aliases (sql-datum--table-aliases-in-statement))
                       ;; Resolve alias → table
                       (resolved (or (cdr (assoc tbl-part aliases
                                                 #'string-equal-ignore-case))
                                     tbl-part))
                       (resolved-key (downcase resolved))
                       (comp-start start))
                  ;; Fetch columns for the resolved table (and prefetch
                  ;; others from aliases).  Only block during explicit
                  ;; completion (TAB) to avoid freezing on idle typing.
                  (let ((all-tables (cons resolved
                                         (cl-remove resolved
                                                    (mapcar #'cdr aliases)
                                                    :test #'string-equal-ignore-case))))
                    (if (sql-datum--explicit-completion-p)
                        (sql-datum--fetch-columns-sync all-tables buf)
                      (dolist (tbl all-tables)
                        (sql-datum--fetch-columns-async
                         tbl buf col-hash
                         (buffer-local-value
                          'sql-datum--columns-pending buf)))))
                  (when (gethash resolved-key col-hash)
                  (list start end
                        (lambda (string pred action)
                          (let* ((cur-col-hash (and buf (buffer-local-value
                                                         'sql-datum--columns buf)))
                                 (tbl-cols (when cur-col-hash
                                             (gethash resolved-key cur-col-hash)))
                                 (qualified (when tbl-cols
                                              (mapcar (lambda (c)
                                                        (concat tbl-part "." c))
                                                      tbl-cols))))
                            (funcall (completion-table-case-fold (or qualified '()))
                                     string pred action)))
                        :exclusive t
                        :annotation-function
                        (lambda (cand)
                          (let* ((cur-details (and buf (buffer-local-value
                                                        'sql-datum--column-details buf)))
                                 (detail-rows (when cur-details
                                                (gethash resolved-key cur-details)))
                                 (col-name (and (string-match-p "\\." cand)
                                                (car (last (split-string cand "\\.")))))
                                 (dtype (when (and detail-rows col-name)
                                          (cl-some
                                           (lambda (row)
                                             (when (and (consp row) (>= (length row) 2)
                                                        (string-equal-ignore-case
                                                         (nth 0 row) col-name))
                                               (nth 1 row)))
                                           detail-rows))))
                            (if dtype
                                (format " [column: %s]" dtype)
                              " [column]")))
                        :exit-function
                        (lambda (cand status)
                          (when (eq status 'finished)
                            (sql-datum--maybe-quote-completed
                             cand comp-start dialect)))))))
              ;; --- Normal identifier completion (fallback) ---
              ;; Returns a DYNAMIC completion table that re-reads live
              ;; state from the SQLi buffer on every query.  This is
              ;; critical because Emacs caches the capf result — if we
              ;; close over a static snapshot of candidates, async-fetched
              ;; columns (and refreshed tables/routines) never appear
              ;; until the user changes text to force a capf re-invocation.
              (when (not xdb-result)
                (let* ((default-schema (and buf (buffer-local-value 'sql-datum--default-schema buf)))
                       (ds-prefix (and default-schema (concat default-schema ".")))
                       (stmt-tables (sql-datum--tables-in-statement))
                       (comp-start start)
                       ;; Shared state: the completion table stores fresh
                       ;; hash sets here so the annotation function (called
                       ;; per-candidate) can classify without rebuilding.
                       (ann-state (make-hash-table :test #'eq)))
                  ;; Fetch columns for uncached tables.  Only block
                  ;; during explicit completion (TAB); idle completion
                  ;; uses whatever is already cached.
                  (sql-datum--trace "capf: normal-completion stmt-tables=%s buf=%s"
                                    stmt-tables buf)
                  (when (and stmt-tables buf
                             (sql-datum--explicit-completion-p))
                    (sql-datum--fetch-columns-sync stmt-tables buf))
                  (list start end
                        (lambda (string pred action)
                          (let* ((cur-tables (and buf (buffer-local-value 'sql-datum--tables buf)))
                                 (cur-schemas (and buf (buffer-local-value 'sql-datum--schemas buf)))
                                 (cur-routines (and buf (buffer-local-value 'sql-datum--routines buf)))
                                 (cur-col-hash (and buf (buffer-local-value 'sql-datum--columns buf)))
                                 (cur-columns (when cur-col-hash
                                                (let (all)
                                                  (maphash (lambda (_k v)
                                                             (setq all (append v all)))
                                                           cur-col-hash)
                                                  (delete-dups all))))
                                 (cur-dbs (and buf (buffer-local-value 'sql-datum--databases buf)))
                                 (cur-dialect (and buf (buffer-local-value 'sql-datum--dialect buf)))
                                 (bare-tables (when ds-prefix
                                                (let (result)
                                                  (dolist (tbl cur-tables)
                                                    (when (string-prefix-p ds-prefix tbl t)
                                                      (push (substring tbl (length ds-prefix)) result)))
                                                  (nreverse result))))
                                 (bare-routines (when ds-prefix
                                                  (let (result)
                                                    (dolist (r cur-routines)
                                                      (when (string-prefix-p ds-prefix r t)
                                                        (push (substring r (length ds-prefix)) result)))
                                                    (nreverse result))))
                                 ;; Re-check for async-fetched columns on every call
                                 (cur-pending (and buf (buffer-local-value
                                                         'sql-datum--columns-pending buf)))
                                 (ctx-columns
                                  (when (and stmt-tables cur-col-hash)
                                    (let (result)
                                      (dolist (tbl stmt-tables)
                                        (let ((cols (gethash (downcase tbl) cur-col-hash)))
                                          (if cols
                                              (setq result (append cols result))
                                            ;; Re-trigger fetch in case earlier attempt
                                            ;; couldn't proceed (prompt not ready yet)
                                            (when (and buf cur-pending)
                                              (sql-datum--fetch-columns-async
                                               tbl buf cur-col-hash cur-pending)))))
                                      (delete-dups result))))
                                 (effective-columns (or ctx-columns cur-columns))
                                 (candidates (if ctx-columns
                                                 (append ctx-columns cur-tables bare-tables cur-schemas
                                                         cur-routines bare-routines
                                                         (when (equal cur-dialect "mssql") cur-dbs))
                                               (append cur-tables bare-tables cur-schemas
                                                       cur-routines bare-routines
                                                       effective-columns
                                                       (when (equal cur-dialect "mssql") cur-dbs))))
                                 ;; Build hash sets for annotation
                                 (tables-set (sql-datum--make-hash-set
                                              (append cur-tables bare-tables)))
                                 (schemas-set (sql-datum--make-hash-set cur-schemas))
                                 (routines-set (sql-datum--make-hash-set
                                                (append cur-routines bare-routines)))
                                 (columns-set (sql-datum--make-hash-set effective-columns))
                                 (ctx-col-set (when ctx-columns
                                                (sql-datum--make-hash-set ctx-columns)))
                                 (dbs-set (when cur-dbs
                                            (sql-datum--make-hash-set cur-dbs)))
                                 (sort-fn (when effective-columns
                                            (lambda (completions)
                                              (let (cols others)
                                                (dolist (c completions)
                                                  (if (gethash c columns-set)
                                                      (push c cols)
                                                    (push c others)))
                                                (nconc (nreverse cols) (nreverse others)))))))
                            ;; Store hash sets for the annotation function
                            (puthash 'tables tables-set ann-state)
                            (puthash 'schemas schemas-set ann-state)
                            (puthash 'routines routines-set ann-state)
                            (puthash 'columns columns-set ann-state)
                            (puthash 'ctx-columns ctx-col-set ann-state)
                            (puthash 'dbs dbs-set ann-state)
                            ;; Delegate to standard completion table
                            (funcall (sql-datum--make-completion-table
                                      candidates sort-fn ds-prefix)
                                     string pred action)))
                        :exclusive t
                        :annotation-function
                        (lambda (cand)
                          (let ((ts (gethash 'tables ann-state))
                                (ss (gethash 'schemas ann-state))
                                (rs (gethash 'routines ann-state))
                                (ccs (gethash 'ctx-columns ann-state))
                                (cs (gethash 'columns ann-state))
                                (ds (gethash 'dbs ann-state)))
                            (cond ((and ts (gethash cand ts))   " [table]")
                                  ((and ss (gethash cand ss))   " [schema]")
                                  ((and rs (gethash cand rs))   " [routine]")
                                  ((and ccs (gethash cand ccs)) " [column]")
                                  ((and cs (gethash cand cs))   " [column]")
                                  ((and ds (gethash cand ds))   " [database]")
                                  (t ""))))
                        :exit-function
                        (lambda (cand status)
                          (when (eq status 'finished)
                            (sql-datum--maybe-quote-completed
                             cand comp-start dialect)
                            ;; Resolve bare→qualified for routine paren check
                            (let* ((cur-rtypes (and buf (buffer-local-value
                                                          'sql-datum--routine-types buf)))
                                   (cur-dialect (and buf (buffer-local-value
                                                           'sql-datum--dialect buf)))
                                   (resolved-cand (if (and ds-prefix
                                                           (not (string-match-p "\\." cand)))
                                                      (concat ds-prefix cand)
                                                    cand)))
                              (when (sql-datum--routine-uses-parens-p
                                     resolved-cand cur-rtypes cur-dialect)
                                (insert "()")
                                (backward-char)
                                (when-let ((msg (sql-datum-eldoc-function)))
                                  (message "%s" msg))))))))))))))))


;;; ---------------------------------------------------------------------------
;;; Eldoc — routine parameter signatures
;;; ---------------------------------------------------------------------------

(defun sql-datum--lookup-signature (ident sigs)
  "Look up IDENT in SIGS hash table with flexible schema matching.
Returns a formatted \"name(params)\" string or nil."
  (when (and ident (not (string-empty-p ident)))
    (let ((key (sql-datum--sigs-canonical-key ident sigs)))
      (when key
        (format "%s(%s)" ident (gethash key sigs))))))

(defun sql-datum--eldoc-search-backward (sigs)
  "Search backward from point for a routine name in SIGS.
Handles cursor after whitespace following a routine name, and
cursor inside parentheses of a function call."
  (or
   ;; Case 1: right after whitespace following the routine name
   ;; e.g. \"my_proc |\" or \"my_proc  |\"
   (save-excursion
     (skip-chars-backward " \t")
     (sql-datum--lookup-signature (sql-datum--identifier-at-point) sigs))
   ;; Case 2: inside parentheses of a function call
   ;; e.g. \"my_func(arg1, |)\"
   (save-excursion
     (let ((paren-pos (nth 1 (syntax-ppss))))
       (when paren-pos
         (goto-char paren-pos)
         (skip-chars-backward " \t")
         (sql-datum--lookup-signature (sql-datum--identifier-at-point) sigs))))
   ;; Case 3: right after ( or after (, without relying on syntax-ppss
   ;; e.g. \"my_func(|\" or typing inside parens with partial content
   (save-excursion
     (skip-chars-backward " \t,a-zA-Z0-9_.@#='\"")
     (when (eq (char-before) ?\()
       (backward-char)
       (skip-chars-backward " \t")
       (sql-datum--lookup-signature (sql-datum--identifier-at-point) sigs)))))

(defun sql-datum-eldoc-function ()
  "Return the parameter signature for the SQL routine at or before point.
Looks up the identifier at point in `sql-datum--routine-signatures'.
When point is not directly on an identifier (e.g. after a space or
inside parentheses), searches backward to find the routine name."
  (when sql-datum-populate-completion
    (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode) (current-buffer))
                    (let ((b (sql-find-sqli-buffer 'datum)))
                      (and b (get-buffer b)))))
           (sigs (and buf (buffer-local-value 'sql-datum--routine-signatures buf))))
      (when (and sigs (> (hash-table-count sigs) 0))
        (or (sql-datum--lookup-signature (sql-datum--identifier-at-point) sigs)
            (sql-datum--eldoc-search-backward sigs))))))

(defun sql-datum--sql-mode-hook ()
  "Hook for `sql-mode' to enable datum completion and keybindings.
The capf function itself checks for an active datum connection and
returns nil if none is found, so this is safe for non-datum buffers."
  (add-hook 'completion-at-point-functions
            #'sql-datum-completion-at-point -90 t)
  (local-set-key (kbd "M-.") #'sql-datum-goto-definition)
  (when sql-datum-populate-completion
    (setq-local eldoc-documentation-function #'sql-datum-eldoc-function)
    (eldoc-mode 1)))

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
          (password (sql-datum--comint-get-password)))
      (unless (and sql-connection parameters)
        (let ((conn-pair (sql-datum--prompt-connection)))
          (setf parameters (car conn-pair))
          (setf password (cdr conn-pair))))
      (unless (or (null password) (string-empty-p password))
        (setf parameters
              (append parameters
                      (if sql-datum-password-variable
                          (progn
                            (setenv sql-datum-password-variable password)
                            (list "--pass"
                                  (format "ENV=%s" sql-datum-password-variable)))
                        (list "--pass" password)))))
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
      ;; Enable eldoc for routine parameter signatures.
      (when sql-datum-populate-completion
        (setq-local eldoc-documentation-function #'sql-datum-eldoc-function)
        (eldoc-mode 1)
        (eldoc-add-command 'delete-backward-char
                           'backward-delete-char-untabify
                           'delete-char
                           'completion-at-point))
      ;; Watch for the first prompt to confirm connection.
      (let ((sqli-buf (current-buffer)))
        (letrec ((watcher
                  (lambda (output)
                    (when (string-match-p (rx bol (* nonl) ">") output)
                      (message "datum: connected.")
                      (remove-hook 'comint-output-filter-functions watcher t)
                      ;; Associate orphan scratch buffers with this connection.
                      (let ((sqli-name (buffer-name sqli-buf)))
                        (sql-datum--trace "connection-watcher: adopting scratch buffers for %s" sqli-name)
                        (dolist (b (buffer-list))
                          (when (and (buffer-live-p b)
                                     (with-current-buffer b
                                       (and (derived-mode-p 'sql-mode)
                                            (null sql-buffer)
                                            (string-match-p "\\*datum-scratch"
                                                            (buffer-name b)))))
                            (sql-datum--trace "  adopted: %s" (buffer-name b))
                            (with-current-buffer b
                              (setq-local sql-buffer sqli-name)))))
                      (when sql-datum-auto-introspect
                        (sql-datum--refresh-async (current-buffer)))
                      (when (and sql-datum-refresh-interval
                                 (not sql-datum--refresh-timer))
                        (setq sql-datum--refresh-timer
                              (run-with-timer sql-datum-refresh-interval
                                              sql-datum-refresh-interval
                                              #'sql-datum--refresh-tick)))))))
          (add-hook 'comint-output-filter-functions watcher nil t)))
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
  "Copy the last query result from the datum buffer to the kill ring.
Output is already in org-mode table format."
  (interactive)
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (unless buf
      (user-error "No active datum buffer found"))
    (with-current-buffer buf
      (save-excursion
        (goto-char (point-max))
        ;; Find the header line above the separator (hline)
        (if (re-search-backward "^|[-+]+" nil t)
            (let* ((header-start (progn (forward-line -1)
                                        (line-beginning-position)))
                   (result-end   (progn
                                   (re-search-forward "^Rows printed:\\|^$" nil t)
                                   (forward-line -1)
                                   ;; Skip trailing blank lines
                                   (while (and (> (point) header-start)
                                               (looking-at-p "^\\s-*$"))
                                     (forward-line -1))
                                   (line-end-position)))
                   (text (buffer-substring-no-properties header-start result-end)))
              (kill-new text)
              (message "datum: last result copied to kill ring (%d chars)"
                       (length text)))
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
    (let ((buf-obj (get-buffer buf)))
      (with-current-buffer buf-obj
        (sql-datum--enqueue
         (list :commands (list (format ":out %s%s" abs-path force-flag)
                               (format "%s;;" query)))))
      (sql-datum--scroll-to-end buf-obj))
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
          (buf-obj (and buf (get-buffer buf)))
          (tables (and buf-obj (buffer-local-value 'sql-datum--tables buf-obj)))
          (ident (sql-datum--identifier-at-point))
          (default (when (and ident tables)
                    (or (cl-find ident tables :test #'string-equal-ignore-case)
                        (let ((ds (and buf-obj (buffer-local-value
                                                'sql-datum--default-schema buf-obj))))
                          (when ds
                            (cl-find (concat ds "." ident) tables
                                     :test #'string-equal-ignore-case))))))
          (table (completing-read (if default
                                      (format "Into table: (default %s) " default)
                                    "Into table: ")
                                  tables nil nil nil nil default))
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
    (let ((buf-obj (get-buffer buf)))
      (with-current-buffer buf-obj
        (sql-datum--enqueue-one
         (format ":in %s %s%s%s" abs-path table-name mode-flag batch-flag)))
      (sql-datum--scroll-to-end buf-obj))
    (message "datum: importing %s into %s%s (batch size %d)"
             (file-name-nondirectory abs-path) table-name
             (or mode-flag " (default)") batch-size)))

(defun sql-datum--scroll-to-end (buf-obj)
  "Scroll all windows displaying BUF-OBJ to the end of the buffer."
  (dolist (win (get-buffer-window-list buf-obj nil t))
    (set-window-point win (with-current-buffer buf-obj (point-max)))))

(defun sql-datum--send-command (cmd &optional silent)
  "Send CMD string to the active datum process.
If the SQLi buffer is not currently visible, display it.
The command is echoed at the process mark so it appears in the
buffer history, making saved sessions easier to follow.
When SILENT is non-nil, skip the echo and buffer display."
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (unless buf
      (user-error "No active datum buffer found"))
    (let* ((buf-obj (get-buffer buf))
           (proc (get-buffer-process buf-obj)))
      (unless silent
        (unless (get-buffer-window buf-obj)
          (display-buffer buf-obj))
        (when proc
          (with-current-buffer buf-obj
            (goto-char (process-mark proc))
            (insert (format "\n>> %s\n" cmd))
            (set-marker (process-mark proc) (point))))
        (sql-datum--scroll-to-end buf-obj))
      (with-current-buffer buf-obj
        (sql-datum--enqueue-one cmd :silent silent)))))

(defun sql-datum--get-dialect ()
  "Return the SQL dialect string from the active datum SQLi buffer."
  (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode) (current-buffer))
                  (let ((b (sql-find-sqli-buffer 'datum)))
                    (and b (get-buffer b))))))
    (and buf (buffer-local-value 'sql-datum--dialect buf))))

(defun sql-datum--read-table (prompt)
  "Read a table name with completion from the introspection cache.
PROMPT is displayed to the user.  If the identifier at point is a
known table, it is offered as the default.  Handles cross-database
three-part names by consulting the xdb-cache and including them
in the candidate list."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (buf-obj (and buf (get-buffer buf)))
         (tables (and buf-obj (buffer-local-value 'sql-datum--tables buf-obj)))
         (xdb-cache (and buf-obj (buffer-local-value 'sql-datum--xdb-cache buf-obj)))
         ;; Collect cross-database tables (already stored as db.schema.table).
         (xdb-tables
          (when (and xdb-cache (> (hash-table-count xdb-cache) 0))
            (let (result)
              (maphash (lambda (_db entry)
                         (dolist (tbl (plist-get entry :tables))
                           (push tbl result)))
                       xdb-cache)
              result)))
         (all-tables (if xdb-tables (append tables xdb-tables) tables))
         (ident (sql-datum--identifier-at-point))
         (parts (and ident (split-string ident "\\." t)))
         (default
          (when (and ident all-tables)
            (or (cl-find ident all-tables :test #'string-equal-ignore-case)
                ;; Bare name may need the default schema prefix to match.
                (let ((ds (and buf-obj (buffer-local-value
                                        'sql-datum--default-schema buf-obj))))
                  (when ds
                    (cl-find (concat ds "." ident) all-tables
                             :test #'string-equal-ignore-case)))
                ;; 3-part name (db.schema.table) — try schema.table in current db.
                (when (>= (length parts) 3)
                  (let ((schema-table (mapconcat #'identity
                                                 (last parts 2) ".")))
                    (cl-find schema-table all-tables
                             :test #'string-equal-ignore-case)))))))
    (completing-read (if default
                         (format "%s(default %s) " prompt default)
                       prompt)
                     all-tables nil nil nil nil default)))

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
  (setq sql-datum--running-quit-flag nil)
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
Prompts with completion from the cached database list.
If the identifier at point starts with a known database name,
it is offered as the default."
  (interactive
   (let* ((buf (sql-find-sqli-buffer 'datum))
          (buf-obj (and buf (get-buffer buf)))
          (databases (and buf-obj (buffer-local-value 'sql-datum--databases
                                                      buf-obj)))
          (ident (sql-datum--identifier-at-point))
          ;; Extract the first dotted segment as a potential database name.
          (first-seg (when ident
                       (car (split-string ident "\\." t))))
          (default (when (and first-seg databases)
                     (cl-find first-seg databases
                              :test #'string-equal-ignore-case))))
     (list (completing-read (if default
                                (format "Switch to database: (default %s) " default)
                              "Switch to database: ")
                            databases nil nil nil nil default))))
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (when buf
      (with-current-buffer (get-buffer buf)
        ;; Clear cross-database cache — context has changed
        (clrhash sql-datum--xdb-cache)
        (letrec ((watcher
                  (lambda (output)
                    (when (string-match-p (rx bol (* nonl) ">") output)
                      (message "datum: connected to %s." db)
                      (remove-hook 'comint-output-filter-functions watcher t)
                      (when sql-datum-auto-introspect
                        (sql-datum--refresh-async (current-buffer)))))))
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
;;; Smart send
;;; ---------------------------------------------------------------------------

(defun sql-datum-ensure-connection ()
  "Ensure the current buffer has a live SQLi connection.
If `sql-buffer' is nil or its process has died, call `sql-connect'
to establish a new connection."
  (interactive)
  (unless (and sql-buffer
               (let ((buffer (get-buffer sql-buffer)))
                 (and buffer
                      (buffer-live-p buffer)
                      (comint-check-proc buffer))))
    (let ((window (selected-window)))
      (call-interactively #'sql-connect)
      (select-window window))
    (when (and sql-buffer (get-buffer sql-buffer))
      (with-current-buffer sql-buffer
        (goto-char (point-max))
        (comint-set-process-mark)))))

(defun sql-datum-send-smart (&optional start end)
  "Send the active region, or the current paragraph if no region is active.
Automatically ensures a live database connection first.
Scrolls the SQLi buffer to the end so output is visible."
  (interactive
   (when mark-active
     (list (region-beginning) (region-end))))
  (sql-datum-ensure-connection)
  (if (and start end (> end start))
      (sql-send-region start end)
    (let ((bounds (save-excursion
                    (let ((beg (progn (backward-paragraph) (point)))
                          (end (progn (forward-paragraph) (point))))
                      (cons beg end)))))
      (when (> (cdr bounds) (car bounds))
        (sql-send-paragraph))))
  (when sql-buffer
    (let ((buf-obj (get-buffer sql-buffer)))
      (when buf-obj
        (sql-datum--scroll-to-end buf-obj)))))

;;; ---------------------------------------------------------------------------
;;; Introspection refresh
;;; ---------------------------------------------------------------------------

(defun sql-datum--refresh-chain (commands buf)
  "Send COMMANDS one at a time to BUF via the command queue.
COMMANDS is a list of strings (e.g. \":refresh-databases\").
The queue sends each command in order, waiting for `ready' between
each.  When the list is exhausted, clears `sql-datum--refresh-in-progress'."
  (when (and (buffer-live-p buf) commands)
    (with-current-buffer buf
      (sql-datum--enqueue
       (list :commands commands
             :silent t
             :priority :low
             :done-fn (lambda ()
                        (when (buffer-live-p buf)
                          (with-current-buffer buf
                            (setq sql-datum--refresh-in-progress nil)))))))))

(defun sql-datum--refresh-async (buf)
  "Start a non-blocking refresh chain in BUF.
Sends the 4 refresh sub-commands one at a time, yielding to the
REPL between each so user input is not blocked."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (if sql-datum--refresh-in-progress
          (message "datum: refresh already in progress")
        (setq sql-datum--refresh-in-progress t)
        (sql-datum--refresh-chain
         '(":refresh-databases"
           ":refresh-schemas"
           ":refresh-tables"
           ":refresh-routines")
         buf)))))

(defun sql-datum-refresh ()
  "Refresh all introspection data (autocomplete candidates).
Uses the async refresh chain so user input is not blocked.
Also re-fetches any cross-database caches built during this session."
  (interactive)
  (let* ((buf (or (and (derived-mode-p 'sql-interactive-mode)
                       (current-buffer))
                  (let ((b (sql-find-sqli-buffer 'datum)))
                    (and b (get-buffer b)))))
         (xdb-cache (and buf (buffer-local-value
                              'sql-datum--xdb-cache buf)))
         (dialect (and buf (buffer-local-value
                            'sql-datum--dialect buf))))
    (when buf
      (sql-datum--refresh-async buf))
    ;; Re-fetch any previously introspected cross-databases
    (when (and xdb-cache (equal dialect "mssql"))
      (let ((dbs-to-refresh nil))
        (maphash (lambda (db _entry) (push db dbs-to-refresh))
                 xdb-cache)
        (when dbs-to-refresh
          (with-current-buffer buf
            (clrhash sql-datum--xdb-cache))
          (dolist (db dbs-to-refresh)
            (sql-datum--send-command (format ":refresh-db %s" db)))
          (message "datum: refreshing introspection + %d cross-db cache(s)..."
                   (length dbs-to-refresh))))))
  (unless (derived-mode-p 'sql-interactive-mode)
    (message "datum: refreshing introspection...")))


(defun sql-datum-toggle-auto-refresh (interval)
  "Toggle periodic introspection refresh.
With a prefix argument, set the INTERVAL in seconds.
Without, toggle on/off using `sql-datum-refresh-interval'
\(default 60 seconds)."
  (interactive "P")
  (if sql-datum--refresh-timer
      (progn
        (cancel-timer sql-datum--refresh-timer)
        (setq sql-datum--refresh-timer nil)
        (message "datum: auto-refresh disabled"))
    (let ((secs (cond ((numberp interval) interval)
                      ((and interval (listp interval))
                       (prefix-numeric-value interval))
                      (sql-datum-refresh-interval
                       sql-datum-refresh-interval)
                      (t 60))))
      (setq sql-datum--refresh-timer
            (run-with-timer secs secs #'sql-datum--refresh-tick))
      (message "datum: auto-refresh every %ds" secs))))

(defun sql-datum--refresh-tick ()
  "Timer callback: send :refresh if a datum process is alive."
  (let ((buf (sql-find-sqli-buffer 'datum)))
    (if (and buf (get-buffer-process (get-buffer buf)))
        (sql-datum--refresh-async (get-buffer buf))
      ;; No live process — stop the timer
      (when sql-datum--refresh-timer
        (cancel-timer sql-datum--refresh-timer)
        (setq sql-datum--refresh-timer nil)))))

;;; ---------------------------------------------------------------------------
;;; Disconnect
;;; ---------------------------------------------------------------------------

(defun sql-datum-disconnect ()
  "Disconnect the current datum session.
Sends :exit to the datum process, kills the SQLi buffer, and
cancels any active refresh timers.  Scratch buffers are kept
but detached, so they can be reused with a new connection."
  (interactive)
  (let ((sqli-name (cond
                    ;; In a SQL editing buffer, use sql-buffer
                    ((and (derived-mode-p 'sql-mode) sql-buffer)
                     sql-buffer)
                    ;; In the SQLi buffer itself
                    ((derived-mode-p 'sql-interactive-mode)
                     (buffer-name (current-buffer)))
                    ;; Fall back to finding any datum SQLi
                    (t (sql-find-sqli-buffer 'datum)))))
    (unless sqli-name
      (user-error "No active datum session"))
    (let ((sqli-buf (get-buffer sqli-name)))
      (unless (and sqli-buf (buffer-live-p sqli-buf))
        (user-error "No active datum session"))
      ;; Cancel refresh timers
      (when sql-datum--refresh-timer
        (cancel-timer sql-datum--refresh-timer)
        (setq sql-datum--refresh-timer nil))
      (when sql-datum--running-timer
        (sql-datum--running-stop-timer))
      ;; Clear command queue and send :exit directly to force shutdown
      (with-current-buffer sqli-buf
        (setq sql-datum--command-queue nil
              sql-datum--queue-current nil
              sql-datum--queue-remaining nil))
      (let ((proc (get-buffer-process sqli-buf)))
        (when (and proc (process-live-p proc))
          (comint-send-string proc ":exit\n")
          ;; Give it a moment to clean up, then force if needed
          (sit-for 0.5)
          (when (process-live-p proc)
            (delete-process proc))))
      ;; Detach scratch buffers so they can be reused with a new connection
      (dolist (buf (buffer-list))
        (when (and (buffer-live-p buf)
                   (with-current-buffer buf
                     (and (derived-mode-p 'sql-mode)
                          (equal sql-buffer sqli-name)
                          (string-match-p "\\*datum-scratch" (buffer-name buf)))))
          (with-current-buffer buf
            (setq-local sql-buffer nil))))
      ;; Kill the SQLi buffer
      (kill-buffer sqli-buf)
      (message "datum: session disconnected"))))

;;; ---------------------------------------------------------------------------
;;; Query templates
;;; ---------------------------------------------------------------------------

(defun sql-datum--prefetch-columns (table)
  "Ensure columns for TABLE are being fetched for completion.
Called by template commands so that column completion is ready
by the time the user presses TAB."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (buf-obj (and buf (get-buffer buf))))
    (when buf-obj
      (let ((col-hash (buffer-local-value 'sql-datum--columns buf-obj))
            (pending  (buffer-local-value 'sql-datum--columns-pending buf-obj)))
        (when (and col-hash pending)
          (sql-datum--fetch-columns-async table buf-obj col-hash pending))))))

(defun sql-datum-insert-select (arg)
  "Insert a SELECT template, prompting for the table name.
With prefix ARG N, limit to N rows (TOP N for MSSQL, LIMIT N for others)."
  (interactive "P")
  (let ((table (sql-datum--read-table "Select from table: "))
        (n (and arg (prefix-numeric-value arg))))
    (sql-datum--prefetch-columns table)
    (if (and n (sql-datum--mssql-p))
        (insert (format "SELECT TOP %d * FROM %s WHERE " n table))
      (insert "SELECT * FROM " table " WHERE ")
      (when n
        (save-excursion (insert (format " LIMIT %d" n)))))))

(defun sql-datum-insert-select-distinct ()
  "Insert a SELECT DISTINCT template, prompting for the table name."
  (interactive)
  (let ((table (sql-datum--read-table "Select distinct from table: ")))
    (sql-datum--prefetch-columns table)
    (insert "SELECT DISTINCT * FROM " table " WHERE ")))

(defun sql-datum-insert-update ()
  "Insert an UPDATE template, prompting for the table name.
Point is left after SET on the SET line."
  (interactive)
  (let ((table (sql-datum--read-table "Update table: ")))
    (sql-datum--prefetch-columns table)
    (insert "UPDATE " table "\nSET ")
    (save-excursion
      (insert "\nWHERE "))))

(defun sql-datum-insert-delete ()
  "Insert a DELETE template, prompting for the table name."
  (interactive)
  (let ((table (sql-datum--read-table "Delete from table: ")))
    (sql-datum--prefetch-columns table)
    (insert "DELETE FROM " table " WHERE ")))

(defun sql-datum-insert-insert ()
  "Insert an INSERT INTO template, prompting for the table name.
If columns are cached for the table, includes them in the template."
  (interactive)
  (let* ((table (sql-datum--read-table "Insert into table: "))
         (buf (sql-find-sqli-buffer 'datum))
         (col-hash (and buf (buffer-local-value 'sql-datum--columns
                                                (get-buffer buf))))
         (cols (and col-hash (gethash (downcase table) col-hash))))
    (unless cols (sql-datum--prefetch-columns table))
    (if cols
        (progn
          (insert "INSERT INTO " table
                  " (" (mapconcat #'identity cols ", ") ") VALUES (")
          (save-excursion (insert ")")))
      (insert "INSERT INTO " table " (")
      (save-excursion (insert ") VALUES ()")))))

(defun sql-datum-insert-select-into ()
  "Insert a SELECT INTO template.
Prompts for destination (free text) and source table (with completion)."
  (interactive)
  (let ((dest (read-string "Destination table: "))
        (source (sql-datum--read-table "Source table: ")))
    (sql-datum--prefetch-columns source)
    (insert "SELECT *\nINTO " dest "\nFROM " source "\nWHERE ")))

(defun sql-datum-insert-join (arg)
  "Insert a JOIN template, prompting for the table name.
With prefix ARG, prompts for join type (LEFT, RIGHT, etc.)."
  (interactive "P")
  (let* ((table (sql-datum--read-table "Join table: "))
         (join-type (if arg
                        (completing-read "Join type: "
                                         '("LEFT" "RIGHT" "INNER" "FULL" "CROSS")
                                         nil t)
                      nil)))
    (sql-datum--prefetch-columns table)
    (insert (if join-type (concat join-type " ") "") "JOIN " table " ON ")))

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
  ;; C-c s f: refresh introspection
  (define-key sql-mode-map (kbd "C-c s f") #'sql-datum-refresh)
  (define-key sql-mode-map (kbd "C-c s F") #'sql-datum-toggle-auto-refresh)
  ;; C-c u: switch database
  (define-key sql-mode-map (kbd "C-c u")   #'sql-datum-use-database)
  ;; C-c C-x: connection management (SLIME-style)
  (define-key sql-mode-map (kbd "C-c C-x c")   #'sql-connect)
  (define-key sql-mode-map (kbd "C-c C-x C-c") #'sql-connect)
  (define-key sql-mode-map (kbd "C-c C-x n")   #'sql-set-sqli-buffer)
  (define-key sql-mode-map (kbd "C-c C-x C-n") #'sql-set-sqli-buffer)
  (define-key sql-mode-map (kbd "C-c C-x s")   #'sql-datum-scratch)
  (define-key sql-mode-map (kbd "C-c C-x C-s") #'sql-datum-scratch)
  (define-key sql-mode-map (kbd "C-c C-x d")   #'sql-datum-disconnect)
  (define-key sql-mode-map (kbd "C-c C-x C-d") #'sql-datum-disconnect)
  ;; C-c s y: copy last result
  (define-key sql-mode-map (kbd "C-c s y") #'sql-datum-copy-last-result)
  ;; C-c i: query templates
  (define-key sql-mode-map (kbd "C-c i s") #'sql-datum-insert-select)
  (define-key sql-mode-map (kbd "C-c i d") #'sql-datum-insert-select-distinct)
  (define-key sql-mode-map (kbd "C-c i u") #'sql-datum-insert-update)
  (define-key sql-mode-map (kbd "C-c i D") #'sql-datum-insert-delete)
  (define-key sql-mode-map (kbd "C-c i i") #'sql-datum-insert-insert)
  (define-key sql-mode-map (kbd "C-c i n") #'sql-datum-insert-select-into)
  (define-key sql-mode-map (kbd "C-c i j") #'sql-datum-insert-join)
  ;; C-c C-c: smart send (region or paragraph, auto-connect)
  (define-key sql-mode-map (kbd "C-c C-c") #'sql-datum-send-smart))

;;; ---------------------------------------------------------------------------
;;; Product registration
;;; ---------------------------------------------------------------------------

;; Force basic completion style for SQL identifiers so that
;; partial-completion/orderless don't treat "." as a word separator.
(add-to-list 'completion-category-overrides
             '(sql-datum-identifier (styles basic)))

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
