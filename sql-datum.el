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

;; The admin wizard forms use the widget library, which is loaded lazily
;; at form-display time.  Pull it in for the byte-compiler only; the
;; functions below are always reached from a live form buffer, where
;; wid-edit is loaded.
(eval-when-compile (require 'wid-edit))
(declare-function widget-forward  "wid-edit" (arg))
(declare-function widget-backward "wid-edit" (arg))
(declare-function widget-field-at "wid-edit" (pos))
(declare-function widget-at        "wid-edit" (&optional pos))
(declare-function widget-button-press "wid-edit" (pos &optional event))
(declare-function widget-field-start "wid-edit" (widget))
(declare-function widget-field-end   "wid-edit" (widget))

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

(defcustom sql-datum-import-batch-size 5000
  "Number of rows per batch for :in imports.
Lower values use less memory; higher values are faster.
Override per-call with a prefix argument to `sql-datum-import'."
  :type 'integer
  :group 'SQL)

(defcustom sql-datum-download-directory nil
  "Where files pulled off a server are kept while being looked at.

nil means a datum directory under `temporary-file-directory\='.  Files
land here when opened with `o\=' in the server filesystem panel, and are
reused when the same file is copied somewhere for keeping, so that
looking at a file and then deciding to keep it fetches it once."
  :type '(choice (const :tag "A directory under the temporary one" nil)
                 directory)
  :group 'SQL)

(defcustom sql-datum-download-limit (* 256 1024 1024)
  "Largest file, in bytes, that will be copied off a server.

A directory is measured whole against this before anything is copied.
The limit is there so that a stray key on a multi-gigabyte backup does
not quietly start pulling it."
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

(defcustom sql-datum-connect-buffer-display 'pop-to-buffer
  "How to display the SQLi buffer after connecting.
Controls what happens to your window/cursor when `sql-connect' creates
a new datum connection.

  `pop-to-buffer'    - Switch to the SQLi buffer (default, standard sql.el behavior).
  `display-buffer'   - Show the SQLi buffer but keep cursor in the current buffer.
  `nil'              - Don't display the SQLi buffer at all."
  :type '(choice (const :tag "Switch to SQLi buffer" pop-to-buffer)
                 (const :tag "Show but stay in current buffer" display-buffer)
                 (const :tag "Don't display" nil))
  :group 'SQL)

(defvar sql-datum-environment nil
  "Per-connection environment variables for the datum subprocess.
An alist of (VARIABLE . VALUE) pairs to set in the process
environment when launching datum.  Set this per-connection in
`sql-connection-alist' to control things like Kerberos tickets:

  (sql-datum-environment ((\"KRB5CCNAME\" . \"/tmp/krb5cc_prod\")))")

(defvar sql-datum-bcp nil
  "Whether to use bcp for bulk CSV export on MSSQL connections.
When non-nil, datum will use the bcp utility instead of pyodbc for
CSV exports.  Set per-connection in `sql-connection-alist':

  (sql-datum-bcp t)")

(defvar sql-datum-bcp-extra nil
  "Per-connection extra arguments for bcp (bulk CSV export).
A list of strings passed as --bcp-extra flags when launching datum.
Set this per-connection in `sql-connection-alist' for servers that
need special bcp flags, e.g. to trust a self-signed certificate:

  (sql-datum-bcp-extra (\"-u\"))")

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

(defvar-local sql-datum--principals nil
  "Login and role names seen in the security panel.

Principals are not part of the introspection the server pushes at
connection time, so this fills in from the security panel instead —
which is what the C-c g keys complete against.")

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
  "Cross-database completion cache.
Hash: database name -> plist with keys :tables :schemas :routines
:routine-types :routine-sigs :pending.
Currently only populated for MSSQL cross-database lookups; same-database
three-part names are synthesized from the main cache for all dialects.")

(defvar-local sql-datum--ready nil
  "Non-nil when the Python process is idle and ready for a command.
Set by the `ready' envelope, cleared when a command is sent.")

(defvar-local sql-datum--columns-fetched (make-hash-table :test #'equal)
  "Set of table keys (downcased) for which column fetching has been attempted.
Prevents repeated fetch attempts for the same table across capf invocations.
Cleared on `sql-datum-refresh'.")

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


(defvar-local sql-datum--silent-in-flight nil
  "Non-nil while a silent command is in-flight (sent but not yet ready).
Set by `sql-datum--queue-send-next' for silent transactions, cleared
by `sql-datum--queue-advance' when `ready' arrives.  The preoutput
filter uses this to strip the PTY echo of silent commands.")

(defvar-local sql-datum--silent-prompt-pending nil
  "Non-nil when a silent command's prompt has not been stripped yet.

The REPL prints a prompt after every command, silent ones included, but
that prompt often lands in a later chunk than the `ready' envelope which
clears `sql-datum--silent-in-flight'.  Keying the strip on that flag
alone let the prompt through whenever nothing else was queued behind it
— precisely the idle case of a panel auto-refreshing.

Set when a silent command's `ready' arrives and cleared once its prompt
is consumed.  Because the process handles one command at a time, the
prompt owed is always the very next thing to arrive, so this stays
narrow enough not to swallow a prompt belonging to the user.")

(defvar-local sql-datum--refresh-in-progress nil
  "Non-nil while an async refresh chain is running.
Prevents overlapping refresh chains.")

(defvar-local sql-datum--bg-pending 0
  "Number of background introspection tasks still in flight.
Set when refresh-async starts, decremented by each bg-ready envelope.
Drives the loading indicator in the mode line.")

(defvar-local sql-datum--bg-total 0
  "Total number of background tasks in the current refresh cycle.
Used with `sql-datum--bg-pending' to show progress like \"loading 2/4\".")

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
          (setq sql-datum--silent-in-flight t))
        (comint-send-string proc (concat cmd "\n"))))))

(defun sql-datum--queue-advance ()
  "Called when a `ready' envelope arrives.  Advance the queue state."
  ;; The prompt for a silent command follows its `ready', so remember
  ;; that one is owed before clearing the in-flight flag.
  (when sql-datum--silent-in-flight
    (setq sql-datum--silent-prompt-pending t))
  (setq sql-datum--silent-in-flight nil)
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

(defvar sql-datum--debug-log-file nil
  "When non-nil, path to a file for debug logging.
Set via `sql-datum-enable-debug-log'.  Captures preoutput filter
inputs and outputs to help diagnose envelope leaking issues.")

(defun sql-datum-enable-debug-log (path)
  "Enable file-based debug logging to PATH.
Logs every preoutput filter call with input/output details.
Use this to diagnose envelope text leaking into the REPL."
  (interactive "FDebug log file: ")
  (setq sql-datum--debug-log-file path)
  (with-temp-buffer
    (insert (format "=== sql-datum debug log started %s ===\n"
                    (format-time-string "%Y-%m-%d %H:%M:%S")))
    (write-region (point-min) (point-max) path nil 'silent))
  (message "datum: debug logging to %s" path))

(defun sql-datum-disable-debug-log ()
  "Disable file-based debug logging."
  (interactive)
  (setq sql-datum--debug-log-file nil)
  (message "datum: debug logging disabled"))

(defun sql-datum--debug-log (fmt &rest args)
  "Write a timestamped line to the debug log file (if enabled)."
  (when sql-datum--debug-log-file
    (let ((msg (apply #'format fmt args))
          (ts (format-time-string "%H:%M:%S.%3N")))
      (write-region (format "[%s] %s\n" ts msg) nil
                    sql-datum--debug-log-file t 'silent))))

(defun sql-datum--preoutput-filter (output)
  "Strip envelope lines from OUTPUT and act on them.
Installed as a `comint-preoutput-filter-functions' hook.
Returns OUTPUT with all ##DATUM:...## lines removed.
Handles partial envelope lines split across multiple filter calls."
  (condition-case err
      (sql-datum--preoutput-filter-1 output)
    (error
     ;; If the filter errors, log it and return output unmodified so
     ;; we can diagnose the issue.  Without this, comint may deregister
     ;; the filter entirely, causing ALL subsequent output to leak.
     (sql-datum--debug-log "FILTER ERROR: %s" err)
     (sql-datum--trace "preoutput-filter ERROR: %s" err)
     (message "datum: preoutput filter error: %s" err)
     output)))

(defun sql-datum--preoutput-filter-1 (output)
  "Inner implementation of the preoutput filter (called via condition-case)."
  (let ((has-datum (string-match-p "##DATUM:" output)))
    (when has-datum
      (sql-datum--debug-log "INPUT len=%d ends-nl=%s partial=%s"
                            (length output)
                            (if (string-suffix-p "\n" output) "y" "n")
                            (if sql-datum--partial-line
                                (format "%d" (length sql-datum--partial-line))
                              "nil"))
      (sql-datum--debug-log "INPUT[0:200]: %s"
                            (substring output 0 (min 200 (length output)))))
    (sql-datum--trace "preoutput-filter: INPUT len=%d has-DATUM=%s ends-newline=%s"
                      (length output)
                      (if has-datum "yes" "no")
                      (if (string-suffix-p "\n" output) "yes" "no")))
  (let ((lines (split-string output "\n"))
        (clean-lines nil))
    ;; Prepend any buffered partial line to the first line
    (when (and sql-datum--partial-line lines)
      (sql-datum--debug-log "PREPEND partial %d chars"
                            (length sql-datum--partial-line))
      (setcar lines (concat sql-datum--partial-line (car lines)))
      (setq sql-datum--partial-line nil))
    ;; Check if the last line is partial (output didn't end with newline).
    ;; A partial line could be:
    ;;  a) An envelope that started but has no closing ## yet.
    ;;  b) A pty split in the middle of "##DATUM:" (e.g., line ends with "##D").
    (let ((last-line (car (last lines))))
      (when (and last-line
                 (not (string-empty-p last-line))
                 (or
                  ;; Has ##DATUM: but no complete envelope → standard partial
                  (and (string-match-p "##DATUM:" last-line)
                       (not (string-match-p "##DATUM:[^:]+:.*?##" last-line)))
                  ;; Ends with a partial "##DATUM:" prefix → pty split mid-marker
                  (string-match-p "##D\\(A\\(T\\(U\\(M:?\\)?\\)?\\)?\\)?\\'" last-line)
                  ;; Ends with # or ## but has no ##DATUM: → pty split at very
                  ;; start of envelope marker.  The PTY can split "##DATUM:"
                  ;; between the two "#" chars, yielding a line ending with
                  ;; just "#" or "##".  Buffer it so the next chunk's "#DATUM:..."
                  ;; gets the missing "#" prepended back.
                  (and (not (string-match-p "##DATUM:" last-line))
                       (string-match-p "#\\{1,2\\}\\'" last-line))))
        (sql-datum--debug-log "BUFFER partial %d chars: %s"
                              (length last-line)
                              (substring last-line 0 (min 80 (length last-line))))
        (setq sql-datum--partial-line last-line)
        (setq lines (butlast lines))))
    (dolist (line lines)
      (cond
       ;; Line contains envelope marker(s) — extract and process ALL matches.
       ;; Multiple envelopes can end up on one line if the newline between
       ;; them was lost in transit (pty buffering, Python I/O layering).
       ((string-match-p "##DATUM:" line)
        (let ((start 0)
              (handled nil))
          (while (string-match sql-datum--envelope-re line start)
            (let ((etype (match-string 1 line))
                  (epayload (match-string 2 line)))
              (sql-datum--debug-log "MATCH %s payload=%d" etype (length epayload))
              (condition-case handler-err
                  (sql-datum--handle-envelope etype epayload)
                (error
                 (sql-datum--debug-log "HANDLER ERROR for %s: %s" etype handler-err))))
            (setq start (match-end 0))
            (setq handled t))
          (unless handled
            (sql-datum--debug-log "SUPPRESS unmatched %d chars: %s"
                                  (length line)
                                  (substring line 0 (min 120 (length line)))))))
       (t (push line clean-lines))))
    (let ((result (string-join (nreverse clean-lines) "\n")))
      ;; Strip echoed command text and prompt from silent commands.
      ;; When a silent command is in-flight, the PTY echoes the command
      ;; (e.g. ":refresh-tables\r\n") and Python emits "\n>".  We strip
      ;; both.  The flag is set by queue-send-next and cleared by
      ;; queue-advance (which runs when the ready envelope is handled
      ;; BEFORE this post-processing), so by the time we get here,
      ;; the flag reflects the NEXT command's state — if another silent
      ;; command was just dispatched, the flag is set again.
      ;;
      ;; We check both the flag AND whether the current queue transaction
      ;; is silent, to catch both the echo (arrives before ready clears
      ;; the flag) and the prompt (arrives after ready, but queue-send-next
      ;; may have already set the flag for the next command).
      (when (or sql-datum--silent-in-flight
                sql-datum--silent-prompt-pending
                (and sql-datum--queue-current
                     (plist-get sql-datum--queue-current :silent)))
        ;; A chunk can hold the echoed command, the prompt owed by the
        ;; previous silent command, or both in either order — the queue
        ;; dispatches the next command as soon as `ready' arrives.  Peel
        ;; whichever is in front until neither is.
        (let ((peeled t))
          (while peeled
            (setq peeled nil)
            (when (and sql-datum--silent-prompt-pending
                       (string-match "\\`[\n\r ]*>[ ]*" result))
              (setq result (substring result (match-end 0))
                    sql-datum--silent-prompt-pending nil
                    peeled t))
            ;; Echoed command text (lines starting with ":").
            (when (string-match "\\`[\n\r]*:[^\n]*[\n\r]*" result)
              (setq result (substring result (match-end 0))
                    peeled t))))
        ;; Anything else arriving first means that prompt is not coming;
        ;; drop the claim rather than hold it against a later prompt of
        ;; the user's own.
        (when (and sql-datum--silent-prompt-pending
                   (not (string-blank-p result)))
          (setq sql-datum--silent-prompt-pending nil)))
      ;; LEAK DETECTION: if result still contains envelope markers,
      ;; something went wrong — log it prominently.
      (when (string-match-p "##DATUM:" result)
        (sql-datum--debug-log "*** LEAK DETECTED *** result len=%d: %s"
                              (length result)
                              (substring result 0 (min 300 (length result))))
        (sql-datum--debug-log "*** Original output len=%d: %s"
                              (length output)
                              (substring output 0 (min 300 (length output)))))
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
         ;; Database changed — clear per-table state and refresh.
         ;; Don't clear lists (schemas, tables, routines) here — the incoming
         ;; "introspect" (replace) envelopes will overwrite them atomically.
         ;; This avoids a window where completion returns nothing while waiting
         ;; for the new data to arrive from the background thread.
         (when (equal key "database")
           (clrhash sql-datum--columns)
           (clrhash sql-datum--column-details)
           (clrhash sql-datum--columns-pending)
           (clrhash sql-datum--columns-fetched)
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
    ("admin-panel"
     (sql-datum--handle-admin-panel payload))
    ("bg-ready"
     (sql-datum--trace "BG-READY: %s" payload)
     (when (> sql-datum--bg-pending 0)
       (cl-decf sql-datum--bg-pending))
     (sql-datum--update-mode-line)
     (when (<= sql-datum--bg-pending 0)
       (message "datum: introspection complete (%d tables, %d routines)"
                (length sql-datum--tables) (length sql-datum--routines))))
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
         (loading  (when (and (boundp 'sql-datum--bg-pending)
                              (> sql-datum--bg-pending 0))
                     (format " loading %d/%d"
                             (- sql-datum--bg-total sql-datum--bg-pending)
                             sql-datum--bg-total)))
         (label    (concat "[datum:" (string-join parts ":")
                           (or loading "") "]")))
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
;;; Admin panels (Activity Monitor, SQL Agent Jobs, SSIS Packages)
;;; ---------------------------------------------------------------------------

(defcustom sql-datum-admin-refresh-interval 5
  "Seconds between auto-refresh of admin panel buffers.
Set to nil to disable auto-refresh."
  :type '(choice (const :tag "Disabled" nil) integer)
  :group 'SQL)

;; Per-panel state is stored as buffer-local variables in each admin buffer.
(defvar-local sql-datum--admin-panel-name nil
  "The panel name for this admin buffer (e.g., \"activity\", \"jobs\", \"ssis\").")

(defvar-local sql-datum--admin-panel-data nil
  "Last received panel data (parsed JSON) for this admin buffer.")

(defvar-local sql-datum--admin-sqli-buf nil
  "The SQLi buffer associated with this admin panel.")

(defvar-local sql-datum--admin-timer nil
  "Auto-refresh timer for this admin buffer.")

(defvar-local sql-datum--admin-fs-marks nil
  "Server paths marked in this listing, as dired marks lines.")

(defvar-local sql-datum--admin-hide-details nil
  "Non-nil to draw only names in a filesystem listing.
`(\=' toggles it, as `dired-hide-details-mode\=' does in dired.")

(defvar sql-datum--admin-display-request nil
  "Panel name the user has explicitly asked to see, or nil.

Panel data arrives asynchronously, so by the time it does there is no
way to tell an auto-refresh from a deliberate request.  The interactive
entry points set this; `sql-datum--admin-show-panel' consumes it.  That
keeps a refresh from dragging a buried panel back into a window while
still letting an explicit request raise one.")

(defvar-local sql-datum--admin-quit-flag nil
  "Non-nil when the user has explicitly quit this admin buffer.")

(defvar-local sql-datum--admin-context nil
  "Context data for sub-panels (e.g., job_name for detail views).")

(defvar-local sql-datum--admin-sort-column nil
  "Column index currently used for sorting, or nil for default order.")

(defvar-local sql-datum--admin-sort-ascending t
  "Non-nil when the current sort is ascending.")

(defvar-local sql-datum--admin-header-line-count 0
  "Number of header lines before the first data row.")

(defvar-local sql-datum--admin-col-positions nil
  "List of (COL-INDEX . START-COLUMN) for cell navigation.")

(defvar-local sql-datum--admin-saved-row-id nil
  "Last known row ID at cursor, saved after each render for stable restore.")

(defvar-local sql-datum--admin-saved-line nil
  "Last known line number at cursor, saved after each render.")

(defvar-local sql-datum--admin-saved-col nil
  "Last known column at cursor, saved after each render.")

(defun sql-datum--admin-buffer-name (panel-name &optional sub-panel)
  "Return the buffer name for PANEL-NAME, with optional SUB-PANEL."
  (if sub-panel
      (format "*datum-admin:%s:%s*" panel-name sub-panel)
    (format "*datum-admin:%s*" panel-name)))

(defun sql-datum--admin-denull (val)
  "Convert JSON :null to nil, leave other values unchanged."
  (if (eq val :null) nil val))

(defun sql-datum--admin-denull-alist (alist)
  "Replace :null values with nil in ALIST (non-recursive)."
  (mapcar (lambda (pair)
            (if (eq (cdr pair) :null)
                (cons (car pair) nil)
              pair))
          alist))

(defun sql-datum--handle-admin-panel (payload)
  "Handle an admin-panel envelope with JSON PAYLOAD."
  (condition-case err
      (let* ((data (sql-datum--admin-denull-alist
                    (json-parse-string payload :object-type 'alist
                                       :array-type 'list)))
             (sub-panel (alist-get 'sub_panel data))
             (sqli-buf (current-buffer)))
        (cond
         ;; Schedule edit form
         ((equal sub-panel "schedule-edit")
          (run-at-time 0 nil #'sql-datum--admin-show-schedule-editor
                       data sqli-buf))
         ;; Step edit form
         ((equal sub-panel "step-edit")
          (run-at-time 0 nil #'sql-datum--admin-show-step-editor
                       data sqli-buf))
         ;; Generic wizard form (databases, security, backup, schema)
         ((equal sub-panel "form")
          (run-at-time 0 nil #'sql-datum--admin-show-form
                       data sqli-buf))
         ;; Server-side directory listing for a path field
         ((equal sub-panel "path-browser")
          (run-at-time 0 nil #'sql-datum--admin-show-path-browser
                       data sqli-buf))
         ;; A file that has just been copied to this machine
         ((equal sub-panel "downloaded")
          (run-at-time 0 nil #'sql-datum--admin-handle-downloaded
                       data sqli-buf))
         ;; A server file's contents, read-only
         ((equal sub-panel "file")
          (run-at-time 0 nil #'sql-datum--admin-show-file
                       data sqli-buf))
         ;; Multi-section detail view (job detail)
         ((alist-get 'sections data)
          (run-at-time 0 nil #'sql-datum--admin-show-detail
                       data sqli-buf))
         ;; Standard tabular panel
         (t
          (run-at-time 0 nil #'sql-datum--admin-show-panel
                       data sqli-buf))))
    (error (message "datum admin-panel error: %s" (error-message-string err)))))

(defun sql-datum--cache-principals (rows sqli-buf)
  "Remember the principal named in each of ROWS, for C-c g completion.
SQLI-BUF is the connection the names belong to."
  (let ((buf (or (and sqli-buf (buffer-live-p sqli-buf) sqli-buf)
                 (let ((b (sql-find-sqli-buffer 'datum)))
                   (and b (get-buffer b))))))
    (when buf
      (with-current-buffer buf
        (setq sql-datum--principals
              (delete-dups
               (delq nil (mapcar (lambda (row)
                                   (let ((name (elt row 0)))
                                     (and name (format "%s" name))))
                                 rows))))))))

(defun sql-datum--admin-show-panel (data sqli-buf)
  "Display admin panel DATA in a dedicated buffer.
SQLI-BUF is the originating SQLi buffer."
  (let* ((panel (alist-get 'panel data))
         (sub-panel (alist-get 'sub_panel data))
         (title (or (alist-get 'title data)
                    (format "datum admin: %s" panel)))
         (headers (alist-get 'headers data))
         (rows (alist-get 'rows data))
         (actions (alist-get 'actions data))
         (info (alist-get 'info data))
         (row-id (alist-get 'row_id data))
         (buf-name (sql-datum--admin-buffer-name panel sub-panel))
         (buf (get-buffer-create buf-name))
         (initial (not (buffer-local-value 'sql-datum--admin-panel-name buf))))
    (when (and (equal panel "security") (null sub-panel) (eql row-id 0))
      (sql-datum--cache-principals rows sqli-buf))
    ;; The listing borrows dired's faces, which only exist once dired
    ;; has been loaded.
    (when (equal panel "filesystem")
      (require 'dired nil t))
    (with-current-buffer buf
      ;; Save cursor state before redraw.  Use the window's point if
      ;; the buffer is visible (works even when the user is in a
      ;; minibuffer/company popup), otherwise fall back to buffer point.
      (let* ((win (get-buffer-window buf t))
             (prev-pos (cond
                        (initial nil)
                        (win (window-point win))
                        (t (point))))
             (saved-row-id (when prev-pos
                             (get-text-property prev-pos 'sql-datum-row-id)))
             (saved-line   (when prev-pos
                             (line-number-at-pos prev-pos)))
             (saved-col    (when prev-pos
                             (save-excursion
                               (goto-char prev-pos)
                               (current-column)))))
        ;; Only set the major mode on first display — calling the mode
        ;; function kills all buffer-local variables, which would wipe
        ;; the timer on every refresh cycle.
        (when initial
          (sql-datum--admin-mode))
        ;; Known before anything is drawn: the rendering depends on
        ;; which panel this is, and `initial' was settled above from
        ;; whether it had been set at all.
        (setq sql-datum--admin-panel-name panel)
        ;; A directory listing invites being mistaken for dired, which
        ;; it is not: it is read-only, it is on another machine, and
        ;; dired's editing keys are not here.  The mode line says so.
        (when (equal panel "filesystem")
          (setq mode-name "datum-fs"))
        ;; Decided before the header is drawn, so the line describing
        ;; auto-refresh can report what is actually running rather than
        ;; what the setting would allow.  A panel may opt out: a
        ;; directory listing is not a thing to poll, any more than dired
        ;; reverts itself.
        (when (and initial (not sub-panel)
                   (not (eq (alist-get 'auto_refresh data) :false)))
          (sql-datum--admin-start-timer buf))
        ;; Apply sort if active
        (when sql-datum--admin-sort-column
          (setq rows (sql-datum--admin-sort-rows
                      rows sql-datum--admin-sort-column
                      sql-datum--admin-sort-ascending)))
        (let ((inhibit-read-only t)
              (header-lines 0))
          (erase-buffer)
          ;; Header
          (insert (propertize title 'face
                              (if (equal panel "filesystem")
                                  'dired-header
                                'bold))
                  "\n")
          (cl-incf header-lines)
          (insert (format "Last refresh: %s" (format-time-string "%H:%M:%S")))
          (if sql-datum--admin-timer
              (insert (format "  [auto-refresh %ds]"
                              sql-datum-admin-refresh-interval))
            (insert "  [auto-refresh off]"))
          (when sql-datum--admin-sort-column
            (insert (format "  [sort: %s %s]"
                            (nth sql-datum--admin-sort-column headers)
                            (if sql-datum--admin-sort-ascending "asc" "desc"))))
          (insert "\n")
          (cl-incf header-lines)
          (when info
            (insert (propertize info 'face 'font-lock-comment-face) "\n")
            (cl-incf header-lines))
          (insert "\n")
          (cl-incf header-lines)
          ;; Table
          (when (and headers (> (length headers) 0))
            (sql-datum--admin-insert-table
             headers rows row-id
             (if sql-datum--admin-hide-details
                 ;; Names only, as dired leaves when details are hidden;
                 ;; a directory is still told apart by its face.
                 '(1)
               (alist-get 'display_columns data)))
            ;; +2 for header row and separator
            (cl-incf header-lines 2))
          (setq sql-datum--admin-header-line-count header-lines)
          ;; Help line
          (insert "\n")
          (sql-datum--admin-insert-help-line actions)
          ;; The buffer was erased, so the marks have to be drawn again.
          (when (equal panel "filesystem")
            (sql-datum--admin-fs-apply-marks)))
        ;; Set buffer-local state after mode init
        (setq sql-datum--admin-panel-data data
              sql-datum--admin-sqli-buf sqli-buf
              sql-datum--admin-context (alist-get 'context data))
        ;; Re-set on every redraw: the keys change with the panel.
        (setq-local header-line-format
                    (sql-datum--admin-header-line
                     actions
                     (append '(("g" . "refresh") ("o" . "sort")
                               ("i" . "inspect"))
                             (when (alist-get 'parent_panel data)
                               '(("B" . "back")))
                             '(("q" . "quit")))))
        ;; Restore cursor position
        (sql-datum--admin-restore-cursor
         saved-row-id saved-line saved-col row-id rows initial)
        ;; Also set window point so the visible cursor moves
        (when-let ((w (get-buffer-window buf t)))
          (set-window-point w (point)))))
    ;; Show the buffer on first creation, or when the user explicitly
    ;; asked for this panel.  A plain auto-refresh must not steal focus
    ;; or drag a buried panel back into a window.
    (let ((requested (equal sql-datum--admin-display-request panel)))
      (when requested
        (setq sql-datum--admin-display-request nil))
      (when (or initial requested)
        (pop-to-buffer buf)))))

(defun sql-datum--admin-restore-cursor (saved-row-id saved-line saved-col
                                        row-id _rows initial)
  "Restore cursor after a panel redraw.
Try to find SAVED-ROW-ID in the new data first (stable across reorder),
fall back to SAVED-LINE/SAVED-COL, or go to first data row if INITIAL.
_ROWS is accepted for interface consistency."
  (cond
   ;; First display — go to first data row
   (initial
    (goto-char (point-min))
    (forward-line sql-datum--admin-header-line-count))
   ;; Try to find the same row by ID
   ((and saved-row-id row-id)
    (goto-char (point-min))
    (let ((found nil))
      (while (and (not found) (not (eobp)))
        (when (equal (get-text-property (point) 'sql-datum-row-id)
                     saved-row-id)
          (setq found t)
          (move-to-column saved-col))
        (unless found (forward-line 1)))
      ;; ID might have disappeared (session ended, etc.)
      (unless found
        (goto-char (point-min))
        (forward-line (min (1- saved-line) (count-lines (point-min) (point-max))))
        (move-to-column saved-col))))
   ;; No row-id — restore by line number
   (saved-line
    (goto-char (point-min))
    (forward-line (min (1- saved-line) (count-lines (point-min) (point-max))))
    (move-to-column saved-col))
   ;; Shouldn't happen, but be safe
   (t (goto-char (point-min)))))

(defun sql-datum--admin-insert-table (headers rows row-id &optional shown)
  "Insert a formatted table with HEADERS and ROWS.
ROW-ID is the column index used as row identifier.

SHOWN, when given, says which columns to draw: a count keeps that many
leading ones, and a list of indices keeps exactly those.  The rest are
still carried on each row — as the row id, and for `i' — but are not
drawn.  The filesystem panel keeps each entry's full path this way
without showing it against every line, the way dired shows the
directory once at the top and bare names below.  Sorting stays
correct because each drawn column remembers which column it really
is."
  (let* ((columns (cond
                   ((null shown) (number-sequence 0 (1- (length headers))))
                   ((listp shown) (seq-filter (lambda (i)
                                                (< i (length headers)))
                                              shown))
                   (t (number-sequence 0 (1- (min shown (length headers)))))))
         (ncols (length columns))
         (all-headers headers)
         (headers (mapcar (lambda (i) (nth i all-headers)) columns))
         ;; Calculate column widths
         (widths (make-vector ncols 0))
         (_ (dotimes (i ncols)
              (aset widths i (length (nth i headers)))))
         (_ (dolist (row rows)
              (dotimes (i ncols)
                (let ((w (length (or (nth (nth i columns) row) ""))))
                  (when (> w (aref widths i))
                    (aset widths i (min w 80)))))))  ; Cap at 80
         (fmt (mapconcat (lambda (w) (format "%%-%ds" w))
                         (append widths nil) "  "))
         (separator (mapconcat (lambda (w) (make-string w ?-))
                               (append widths nil) "  "))
         ;; Compute column start positions for click detection
         (col-starts (let ((pos 0) starts)
                       (dotimes (i ncols)
                         ;; Keyed by the column's real index, so
                         ;; clicking a header sorts that column even
                         ;; when the ones before it are not drawn.
                         (push (cons (nth i columns) pos) starts)
                         (setq pos (+ pos (aref widths i) 2)))
                       (nreverse starts))))
    ;; Store column positions as buffer-local for cell navigation
    (setq sql-datum--admin-col-positions col-starts)
    ;; Header row — each column header is clickable for sorting
    (let ((header-start (point)))
      (insert (apply #'format fmt
                     (cl-loop for h in headers
                              for i in columns
                              collect (let ((indicator
                                            (cond
                                             ((not (eql i sql-datum--admin-sort-column)) "")
                                             (sql-datum--admin-sort-ascending " ^")
                                             (t " v"))))
                                        (concat h indicator)))))
      (put-text-property header-start (point) 'face 'bold)
      ;; Store column positions on header line for click-to-sort
      (put-text-property header-start (point)
                         'sql-datum-col-starts col-starts)
      (insert "\n"))
    (insert separator "\n")
    ;; Data rows
    (if (null rows)
        (insert (propertize "(no data)" 'face 'font-lock-comment-face) "\n")
      (let ((row-index 0))
        (dolist (row rows)
          ;; Pad row if needed
          (let* ((needed (1+ (apply #'max (or row-id 0) columns)))
                 (padded (append row
                                 (make-list (max 0 (- needed (length row)))
                                            ""))))
            ;; Truncate cells to their column width
            (let ((display-row
                   (cl-loop for column in columns
                            for i from 0
                            collect (let ((cell (or (nth column padded) ""))
                                          (w (aref widths i)))
                                      (if (> (length cell) w)
                                          (concat (substring cell 0 (max 0 (- w 3))) "...")
                                        cell)))))
              (let ((line-start (point))
                    (id-val (and row-id (nth row-id padded))))
                (insert (apply #'format fmt display-row))
                ;; Add text properties for row identification and data
                (when id-val
                  (put-text-property line-start (point) 'sql-datum-row-id id-val))
                (put-text-property line-start (point) 'sql-datum-row-index row-index)
                (put-text-property line-start (point) 'sql-datum-row-data padded)
                ;; Color-code status columns.  A directory listing is
                ;; read by its own conventions, so it is faced the way
                ;; dired faces one rather than by status keyword — "No"
                ;; is a plausible file name, not a disabled job.
                (if (equal sql-datum--admin-panel-name "filesystem")
                    (sql-datum--admin-colorize-fs-row
                     line-start (point) padded)
                  (sql-datum--admin-colorize-row line-start (point) padded))
                (insert "\n"))))
          (cl-incf row-index))))))

(defun sql-datum--admin-colorize-fs-row (start _end cells)
  "Face the name in a filesystem row the way dired faces it.

Dired puts `dired-directory' on a directory's name, and that is the
cue people read a listing by, so this listing uses the same face
rather than inventing a colour of its own."
  (let ((name-col (alist-get 1 sql-datum--admin-col-positions)))
    (when (and name-col (equal (nth 0 cells) "dir"))
      (let* ((from (+ start name-col))
             (to (min (+ from (length (nth 1 cells)))
                      (line-end-position))))
        (when (< from to)
          (put-text-property from to 'face 'dired-directory))))))

(defun sql-datum--admin-colorize-row (start end cells)
  "Apply color to the row from START to END based on CELLS content."
  (let ((status-keywords
         '(("Failed" . compilation-error)
           ("Canceled" . font-lock-warning-face)
           ("Ended unexpectedly" . compilation-error)
           ("Running" . compilation-info)
           ("In Progress" . compilation-info)
           ("Succeeded" . success)
           ("Completed" . success)
           ("No" . font-lock-comment-face)  ; Disabled
           ("suspended" . font-lock-warning-face)
           ("sleeping" . font-lock-comment-face))))
    (dolist (cell cells)
      (let ((face-entry (assoc cell status-keywords)))
        (when face-entry
          (put-text-property start end 'face (cdr face-entry)))))))

(defun sql-datum--admin-header-line (actions &optional extra)
  "Return a header line describing ACTIONS and the common keys.

A panel long enough to scroll puts its help off-screen, which is where
it is least useful.  A header line stays pinned to the top of the window
and costs no buffer lines, so `sql-datum--admin-header-line-count' and
the cursor restore are unaffected."
  (let ((parts nil))
    (dolist (action actions)
      (let ((key (alist-get 'key action))
            (label (alist-get 'label action)))
        (when (and key label)
          (push (concat (propertize key 'face 'bold) " " label) parts))))
    (dolist (pair (or extra '(("g" . "refresh") ("q" . "quit"))))
      (push (concat (propertize (car pair) 'face 'bold) " " (cdr pair))
            parts))
    ;; Panel actions lead, so a narrow window truncates the generic keys
    ;; rather than the ones specific to what is on screen.
    (concat " " (mapconcat #'identity (nreverse parts) "   "))))

(defun sql-datum--admin-insert-help-line (_actions)
  "Insert the navigation help below the table.

The actions and the keys reached for most often live in the header line,
which stays visible however far the table scrolls.  What is left here is
the navigation detail worth reading once."
  (insert (propertize "Nav: " 'face 'font-lock-comment-face)
          "arrows=cell  TAB/S-TAB=next/prev cell  n/p=row  "
          "RET=sort(header)/detail(row)  "
          (propertize "a" 'face 'bold) "=auto-refresh\n"))

;; --- Sorting ---

(defun sql-datum--admin-numeric-string-p (s)
  "Return non-nil if S is a string representing a number."
  (and (not (string-empty-p s))
       (string-match-p "\\`-?[0-9]+\\(?:\\.[0-9]*\\)?\\'" s)))

(defun sql-datum--admin-sort-rows (rows col-index ascending)
  "Sort ROWS by COL-INDEX.  ASCENDING controls direction.
Uses numeric comparison when all non-empty values in the column are
numbers, otherwise string comparison."
  (let* ((sorted (copy-sequence rows))
         ;; Probe the column to decide comparison mode
         (all-numeric t))
    (dolist (row sorted)
      (let ((v (or (nth col-index row) "")))
        (when (and (not (string-empty-p v))
                   (not (sql-datum--admin-numeric-string-p v)))
          (setq all-numeric nil))))
    (sort sorted
          (lambda (a b)
            (let ((va (or (nth col-index a) ""))
                  (vb (or (nth col-index b) "")))
              ;; Empty strings sort last regardless of direction
              (cond
               ((and (string-empty-p va) (string-empty-p vb)) nil)
               ((string-empty-p va) nil)  ; a sorts after b
               ((string-empty-p vb) t)    ; a sorts before b
               (all-numeric
                (let ((na (string-to-number va))
                      (nb (string-to-number vb)))
                  (if ascending (< na nb) (> na nb))))
               (t
                (if ascending
                    (string< va vb)
                  (string< vb va)))))))
    sorted))

(defun sql-datum-admin-sort-by-column (col-index)
  "Sort the current panel by column COL-INDEX.
Toggles direction if already sorting by this column."
  (if (eql col-index sql-datum--admin-sort-column)
      ;; Toggle direction, or clear sort on third press
      (if sql-datum--admin-sort-ascending
          (setq sql-datum--admin-sort-ascending nil)
        (setq sql-datum--admin-sort-column nil
              sql-datum--admin-sort-ascending t))
    ;; New column
    (setq sql-datum--admin-sort-column col-index
          sql-datum--admin-sort-ascending t))
  ;; Re-render with current data
  (when sql-datum--admin-panel-data
    (sql-datum--admin-show-panel sql-datum--admin-panel-data
                                 sql-datum--admin-sqli-buf)))

(defun sql-datum-admin-sort ()
  "Prompt for a column to sort by."
  (interactive)
  (let* ((data sql-datum--admin-panel-data)
         (headers (alist-get 'headers data)))
    (unless headers (user-error "No data to sort"))
    (let* ((choices (cl-loop for h in headers
                             for i from 0
                             collect (cons (format "%d: %s%s" (1+ i) h
                                                   (cond
                                                    ((not (eql i sql-datum--admin-sort-column)) "")
                                                    (sql-datum--admin-sort-ascending " [asc]")
                                                    (t " [desc]")))
                                           i)))
           (choice (completing-read "Sort by column: " choices nil t))
           (col (cdr (assoc choice choices))))
      (when col
        (sql-datum-admin-sort-by-column col)))))

(defun sql-datum-admin-click-sort ()
  "Sort by the column under the cursor on the header line."
  (interactive)
  ;; Are we on the header line?
  (let ((col-starts (get-text-property (line-beginning-position)
                                       'sql-datum-col-starts)))
    (if col-starts
        ;; Find which column the cursor is in
        (let ((cur-col (current-column))
              (target-col nil))
          (dolist (entry col-starts)
            (when (<= (cdr entry) cur-col)
              (setq target-col (car entry))))
          (when target-col
            (sql-datum-admin-sort-by-column target-col)))
      ;; Not on header line — treat RET normally
      (sql-datum-admin-detail))))

;; --- Row inspection ---

(defun sql-datum-admin-inspect ()
  "Show all column values for the row at point in a popup."
  (interactive)
  (let ((row-data (get-text-property (line-beginning-position) 'sql-datum-row-data))
        (headers (alist-get 'headers sql-datum--admin-panel-data)))
    (unless row-data (user-error "No data row at point"))
    (let ((buf (get-buffer-create "*datum-admin:inspect*")))
      (with-current-buffer buf
        (let ((inhibit-read-only t))
          (erase-buffer)
          (insert (propertize "Row Detail" 'face 'bold) "\n")
          (insert (make-string 40 ?-) "\n")
          (cl-loop for h in headers
                   for v in row-data
                   for i from 0
                   do (insert (propertize (format "%-20s" h)
                                          'face 'font-lock-function-name-face)
                              " " (or v "") "\n"))
          (insert "\n" (propertize "Press q to close" 'face 'font-lock-comment-face) "\n"))
        (special-mode)
        (goto-char (point-min)))
      (display-buffer buf
                      '((display-buffer-below-selected)
                        (window-height . fit-window-to-buffer))))))

(defun sql-datum--admin-show-detail (data sqli-buf)
  "Display a multi-section detail view from DATA.
SQLI-BUF is the originating SQLi buffer."
  (let* ((panel (alist-get 'panel data))
         (sub-panel (or (alist-get 'sub_panel data) "detail"))
         (title (or (alist-get 'title data)
                    (format "datum admin: %s detail" panel)))
         (sections (alist-get 'sections data))
         (info (alist-get 'info data))
         (buf-name (sql-datum--admin-buffer-name panel sub-panel))
         (buf (get-buffer-create buf-name))
         (initial (not (buffer-local-value 'sql-datum--admin-panel-name buf))))
    (with-current-buffer buf
      (let ((saved-line (unless initial (line-number-at-pos)))
            (saved-col (unless initial (current-column))))
        (when initial
          (sql-datum--admin-mode))
        (let ((inhibit-read-only t))
          (erase-buffer)
          (insert (propertize title 'face 'bold) "\n")
          (insert (format "Last refresh: %s\n" (format-time-string "%H:%M:%S")))
          (when info
            (insert (propertize info 'face 'font-lock-comment-face) "\n"))
          (insert "\n")
          ;; Render each section
          (dolist (section sections)
            (let ((sec-title (alist-get 'title section))
                  (sec-headers (alist-get 'headers section))
                  (sec-rows (alist-get 'rows section))
                  (sec-row-id (alist-get 'row_id section)))
              (insert (propertize (concat "--- " sec-title " ---")
                                  'face 'font-lock-function-name-face
                                  'sql-datum-section sec-title) "\n")
              (when (and sec-headers (> (length sec-headers) 0))
                (let ((table-start (point)))
                  (sql-datum--admin-insert-table sec-headers sec-rows sec-row-id)
                  (put-text-property table-start (point)
                                     'sql-datum-section sec-title)))
              (insert "\n")))
          ;; The keys live in the header line, which stays visible
          ;; however far the sections scroll.
          (insert (propertize "Nav: " 'face 'font-lock-comment-face)
                  "n/p=row  arrows=cell\n"))
        (setq sql-datum--admin-panel-name panel
              sql-datum--admin-panel-data data
              sql-datum--admin-sqli-buf sqli-buf
              sql-datum--admin-context (alist-get 'context data))
        ;; A detail view collects the actions of all its sections.
        (setq-local header-line-format
                    (sql-datum--admin-header-line
                     (apply #'append
                            (mapcar (lambda (s) (alist-get 'actions s))
                                    sections))
                     '(("g" . "refresh") ("i" . "inspect")
                       ("B" . "back") ("q" . "quit"))))
        (if saved-line
            (progn
              (goto-char (point-min))
              (forward-line (min (1- saved-line)
                                 (count-lines (point-min) (point-max))))
              (move-to-column saved-col))
          (goto-char (point-min))
          (forward-line 4))))
    (display-buffer buf)))

;; --- Admin mode ---

(defvar sql-datum--admin-mode-map
  (let ((map (make-sparse-keymap)))
    ;; Cell navigation — arrow keys jump between cells
    (define-key map (kbd "<left>")  #'sql-datum-admin-cell-left)
    (define-key map (kbd "<right>") #'sql-datum-admin-cell-right)
    (define-key map (kbd "<up>")    #'sql-datum-admin-cell-up)
    (define-key map (kbd "<down>")  #'sql-datum-admin-cell-down)
    ;; Tab/S-Tab cycle columns (wrapping to next/prev row)
    (define-key map (kbd "TAB")     #'sql-datum-admin-next-cell)
    (define-key map (kbd "<backtab>") #'sql-datum-admin-prev-cell)
    ;; Row navigation
    (define-key map "n" #'sql-datum-admin-next-row)
    (define-key map "p" #'sql-datum-admin-prev-row)
    ;; General
    (define-key map "q" #'sql-datum-admin-quit)
    (define-key map "g" #'sql-datum-admin-refresh)
    (define-key map "a" #'sql-datum-admin-toggle-auto-refresh)
    (define-key map "B" #'sql-datum-admin-back)
    ;; Sorting
    (define-key map "o" #'sql-datum-admin-sort)
    (define-key map (kbd "RET") #'sql-datum-admin-click-sort)
    ;; Inspection
    (define-key map "i" #'sql-datum-admin-inspect)
    ;; Activity monitor
    (define-key map "k" #'sql-datum-admin-kill-session)
    ;; Jobs panel
    ;; s starts a job, but cycles the sort in a directory listing.
    (define-key map "s" #'sql-datum-admin-start-or-sort)
    ;; S stops a job, but shrinks a file in the database files view.
    (define-key map "S" #'sql-datum-admin-stop-or-shrink)
    ;; e toggles a job, but opens the entry at point in the filesystem
    ;; panel, where dired has taught the hand that e means open.
    (define-key map "e" #'sql-datum-admin-enable-or-open)
    (define-key map "H" #'sql-datum-admin-job-history)
    (define-key map "d" #'sql-datum-admin-detail)
    ;; SSIS panel
    (define-key map "r" #'sql-datum-admin-run-package)
    ;; Contextual editing (steps or schedules)
    (define-key map "E" #'sql-datum-admin-edit-at-point)
    (define-key map "N" #'sql-datum-admin-new-at-point)
    (define-key map "D" #'sql-datum-admin-delete-at-point)
    ;; Databases panel: file management and backups
    (define-key map "F" #'sql-datum-admin-database-files)
    (define-key map "K" #'sql-datum-admin-database-backups)
    ;; Schema panel
    (define-key map "T" #'sql-datum-admin-schema-tables)
    ;; R restores a backup, but revokes in the permissions view.
    (define-key map "R" #'sql-datum-admin-restore-or-revoke)
    ;; Security panel: database user mappings and permissions
    (define-key map "U" #'sql-datum-admin-user-mappings)
    (define-key map "P" #'sql-datum-admin-permissions)
    (define-key map "G" #'sql-datum-admin-grant)
    ;; Filesystem panel.  f, s and ( are dired's keys for the same
    ;; jobs; s and ( are free, so only s needs sharing with the jobs
    ;; panel, where it starts a job.
    (define-key map "f" #'sql-datum-admin-open-path)
    ;; o and U already meant something: o prompts for a column to sort
    ;; by, and U shows a login's database users.  In a listing they are
    ;; dired's keys instead.  C, m, u and t were free.
    (define-key map "o" #'sql-datum-admin-open-or-sort)
    (define-key map "U" #'sql-datum-admin-unmark-or-mappings)
    (define-key map "C" #'sql-datum-admin-fs-copy)
    (define-key map "m" #'sql-datum-admin-fs-mark)
    (define-key map "u" #'sql-datum-admin-fs-unmark)
    (define-key map "t" #'sql-datum-admin-fs-toggle-marks)
    (define-key map "(" #'sql-datum-admin-toggle-details)
    (define-key map "^" #'sql-datum-admin-parent-directory)
    (define-key map "v" #'sql-datum-admin-view-file)
    (define-key map "w" #'sql-datum-admin-copy-path)
    ;; Query text (activity panel)
    (define-key map (kbd "M-.") #'sql-datum-admin-query-text)
    map)
  "Keymap for datum admin panel buffers.")

(define-derived-mode sql-datum--admin-mode special-mode "datum-admin"
  "Major mode for datum admin panel buffers."
  (setq buffer-read-only t
        truncate-lines t)
  (add-hook 'post-command-hook #'sql-datum--admin-save-cursor nil t))

(defun sql-datum--admin-save-cursor ()
  "Save current cursor position to buffer-locals for stable refresh restore."
  (setq sql-datum--admin-saved-row-id (sql-datum--admin-row-id-at-point)
        sql-datum--admin-saved-line (line-number-at-pos)
        sql-datum--admin-saved-col (current-column)))

;; --- Admin commands ---

(defun sql-datum-admin-quit ()
  "Stop auto-refresh and close the admin panel buffer."
  (interactive)
  (sql-datum--admin-stop-timer (current-buffer))
  (setq sql-datum--admin-quit-flag t)
  (quit-window t))

(defun sql-datum-admin-refresh ()
  "Manually refresh the current admin panel."
  (interactive)
  (sql-datum--admin-send-refresh))

(defun sql-datum-admin-toggle-auto-refresh ()
  "Toggle auto-refresh of the current admin panel."
  (interactive)
  (if sql-datum--admin-timer
      (progn
        (sql-datum--admin-stop-timer (current-buffer))
        (message "datum admin: auto-refresh disabled"))
    (sql-datum--admin-start-timer (current-buffer))
    (message "datum admin: auto-refresh enabled (%ds interval)"
             sql-datum-admin-refresh-interval)))

(defun sql-datum-admin-back ()
  "Go back to the parent panel."
  (interactive)
  (let ((parent (alist-get 'parent_panel sql-datum--admin-panel-data)))
    (if parent
        (progn
          (setq sql-datum--admin-display-request parent)
          (sql-datum--admin-send-command
           (format ":admin %s" parent)))
      (message "datum admin: no parent panel"))))

(defun sql-datum--admin-navigable-line-p ()
  "Return non-nil if the current line is a data row or the header row."
  (or (get-text-property (point) 'sql-datum-row-data)
      (get-text-property (point) 'sql-datum-col-starts)))

(defun sql-datum-admin-next-row ()
  "Move to the next data or header row."
  (interactive)
  (let ((col (current-column)))
    (forward-line 1)
    (while (and (not (eobp))
                (not (sql-datum--admin-navigable-line-p)))
      (forward-line 1))
    (move-to-column col)
    (sql-datum--admin-ensure-cell-visible)))

(defun sql-datum-admin-prev-row ()
  "Move to the previous data or header row."
  (interactive)
  (let ((col (current-column)))
    (forward-line -1)
    (while (and (not (bobp))
                (not (sql-datum--admin-navigable-line-p)))
      (forward-line -1))
    (move-to-column col)
    (sql-datum--admin-ensure-cell-visible)))

(defun sql-datum--admin-current-col-index ()
  "Return the column index the cursor is in, based on col-positions."
  (when sql-datum--admin-col-positions
    (let ((cur (current-column))
          (result nil))
      (dolist (entry sql-datum--admin-col-positions)
        (when (<= (cdr entry) cur)
          (setq result (car entry))))
      result)))

(defun sql-datum-admin-next-cell ()
  "Move to the next column in the current row."
  (interactive)
  (unless sql-datum--admin-col-positions
    (user-error "No table data"))
  (let* ((cur (current-column))
         (next-pos nil))
    ;; Find the first column start that is after current position
    (dolist (entry sql-datum--admin-col-positions)
      (when (and (> (cdr entry) cur)
                 (or (null next-pos) (< (cdr entry) next-pos)))
        (setq next-pos (cdr entry))))
    (if next-pos
        (move-to-column next-pos)
      ;; Wrap to first column of next row
      (sql-datum-admin-next-row)
      (move-to-column (cdar sql-datum--admin-col-positions)))
    (sql-datum--admin-ensure-cell-visible)))

(defun sql-datum-admin-prev-cell ()
  "Move to the previous column in the current row."
  (interactive)
  (unless sql-datum--admin-col-positions
    (user-error "No table data"))
  (let* ((cur (current-column))
         (prev-pos nil))
    ;; Find the last column start that is before current position
    (dolist (entry sql-datum--admin-col-positions)
      (when (< (cdr entry) cur)
        (setq prev-pos (cdr entry))))
    (if prev-pos
        (move-to-column prev-pos)
      ;; Wrap to last column of previous row
      (sql-datum-admin-prev-row)
      (move-to-column (cdar (last sql-datum--admin-col-positions))))
    (sql-datum--admin-ensure-cell-visible)))

(defun sql-datum-admin-cell-up ()
  "Move to the same column in the previous data or header row."
  (interactive)
  (sql-datum-admin-prev-row))

(defun sql-datum-admin-cell-down ()
  "Move to the same column in the next data or header row."
  (interactive)
  (sql-datum-admin-next-row))

(defun sql-datum-admin-cell-left ()
  "Move to the previous column, staying on the same row."
  (interactive)
  (unless sql-datum--admin-col-positions
    (user-error "No table data"))
  (let* ((cur (current-column))
         (prev-pos nil))
    (dolist (entry sql-datum--admin-col-positions)
      (when (< (cdr entry) cur)
        (setq prev-pos (cdr entry))))
    (when prev-pos
      (move-to-column prev-pos)
      (sql-datum--admin-ensure-cell-visible))))

(defun sql-datum-admin-cell-right ()
  "Move to the next column, staying on the same row."
  (interactive)
  (unless sql-datum--admin-col-positions
    (user-error "No table data"))
  (let* ((cur (current-column))
         (next-pos nil))
    (dolist (entry sql-datum--admin-col-positions)
      (when (and (> (cdr entry) cur)
                 (or (null next-pos) (< (cdr entry) next-pos)))
        (setq next-pos (cdr entry))))
    (when next-pos
      (move-to-column next-pos)
      (sql-datum--admin-ensure-cell-visible))))

(defun sql-datum--admin-ensure-cell-visible ()
  "Scroll horizontally so the current cell is visible in the window.
Shows the cell start and tries to reveal as much of the cell as possible."
  (when-let ((win (get-buffer-window (current-buffer))))
    (let* ((col (current-column))
           (win-width (window-body-width win))
           (hscroll (window-hscroll win))
           ;; Find end of current cell (next column start or end of line)
           (cell-end col))
      ;; Find the next column start to determine cell width
      (when sql-datum--admin-col-positions
        (let ((next nil))
          (dolist (entry sql-datum--admin-col-positions)
            (when (and (> (cdr entry) col)
                       (or (null next) (< (cdr entry) next)))
              (setq next (cdr entry))))
          (setq cell-end (or next (save-excursion (end-of-line) (current-column))))))
      ;; Scroll so that cell start is visible and as much of cell as possible
      (cond
       ;; Cell start is off-screen to the left
       ((< col hscroll)
        (set-window-hscroll win col))
       ;; Cell end is off-screen to the right
       ((> cell-end (+ hscroll win-width -1))
        (set-window-hscroll win (max 0 (- cell-end win-width -1))))))))

(defun sql-datum--admin-row-id-at-point ()
  "Get the row ID at point, or nil."
  (get-text-property (line-beginning-position) 'sql-datum-row-id))

(defun sql-datum--admin-row-cells-at-point ()
  "Get all cell values for the row at point.
Returns a list of strings by parsing the current line against column widths."
  (let* ((data sql-datum--admin-panel-data)
         (rows (alist-get 'rows data)))
    ;; Find the row by row-id
    (let ((row-id (sql-datum--admin-row-id-at-point))
          (row-id-col (alist-get 'row_id data)))
      (when (and row-id row-id-col rows)
        (cl-find-if (lambda (row) (equal (nth row-id-col row) row-id)) rows)))))

;; --- Action commands ---

(defun sql-datum-admin-kill-session ()
  "Kill the session/process at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "activity")
    (user-error "Kill is only available in the activity panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No session at point"))
    (when (yes-or-no-p (format "Kill session %s? " id))
      (sql-datum--admin-send-command
       (format ":admin-action activity kill %s" id)))))

(defun sql-datum-admin-query-text ()
  "Show the full query text for the session at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "activity")
    (user-error "Query text is only available in the activity panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No session at point"))
    (xref-push-marker-stack)
    (sql-datum--admin-send-command
     (format ":admin-action activity query-text %s" id))))

(defun sql-datum-admin-start-job ()
  "Start the SQL Agent job at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Start job is only available in the jobs panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No job at point"))
    (when (yes-or-no-p (format "Start job '%s'? " id))
      (sql-datum--admin-send-command
       (format ":admin-action jobs start-job %s" id)))))

(defun sql-datum-admin-stop-job ()
  "Stop the SQL Agent job at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Stop job is only available in the jobs panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No job at point"))
    (when (yes-or-no-p (format "Stop job '%s'? " id))
      (sql-datum--admin-send-command
       (format ":admin-action jobs stop-job %s" id)))))

(defun sql-datum-admin-toggle-enable ()
  "Toggle enable/disable for the SQL Agent job at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Toggle enable is only available in the jobs panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No job at point"))
    (sql-datum--admin-send-command
     (format ":admin-action jobs toggle-enable %s" id))))

(defun sql-datum-admin-detail ()
  "Show detail for the item at point."
  (interactive)
  (let ((id (sql-datum--admin-row-id-at-point))
        (panel sql-datum--admin-panel-name))
    (unless id (user-error "No item at point"))
    (cond
     ((equal panel "jobs")
      (sql-datum--admin-send-command
       (format ":admin jobs detail %s" id)))
     ((equal panel "ssis")
      (sql-datum--admin-send-command
       (format ":admin ssis executions %s" id)))
     ((equal panel "filesystem")
      (sql-datum-admin-open-path))
     (t (message "No detail view for this panel")))))

(defun sql-datum-admin-new-job ()
  "Create a SQL Agent job."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Not in the jobs panel"))
  (sql-datum--admin-send-command ":admin-action jobs new-job"))

(defun sql-datum-admin-edit-job ()
  "Edit the properties of the job at point.

Its steps and schedules are edited from the detail view (`d\='); these
are the job's own properties — name, owner, category, notifications."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Not in the jobs panel"))
  (let ((name (or (sql-datum--admin-row-id-at-point)
                  (alist-get 'job_name sql-datum--admin-context))))
    (unless name (user-error "No job at point"))
    (sql-datum--admin-send-command
     (format ":admin-action jobs edit-job %s" name))))

(defun sql-datum-admin-delete-job ()
  "Delete the job at point, by way of the panel that says what goes."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "Not in the jobs panel"))
  (let ((name (or (sql-datum--admin-row-id-at-point)
                  (alist-get 'job_name sql-datum--admin-context))))
    (unless name (user-error "No job at point"))
    (setq sql-datum--admin-display-request "jobs")
    (sql-datum--admin-send-command
     (format ":admin-action jobs drop-job-check %s" name))))

(defun sql-datum-admin-job-history ()
  "Show execution history for the SQL Agent job at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "jobs")
    (user-error "History is only available in the jobs panel"))
  (let ((id (sql-datum--admin-row-id-at-point)))
    (unless id (user-error "No job at point"))
    (sql-datum--admin-send-command
     (format ":admin jobs history %s" id))))

(defun sql-datum-admin-run-package ()
  "Run the SSIS package at point."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "ssis")
    (user-error "Run package is only available in the SSIS panel"))
  (let ((cells (sql-datum--admin-row-cells-at-point)))
    (unless cells (user-error "No package at point"))
    (let ((folder (nth 0 cells))
          (project (nth 1 cells))
          (package (nth 2 cells)))
      (when (yes-or-no-p (format "Run SSIS package '%s/%s/%s'? "
                                 folder project package))
        (sql-datum--admin-send-command
         (format ":admin-action ssis run-package %s %s %s"
                 folder project package))))))

;; --- Contextual edit/new/delete dispatch ---

(defun sql-datum-admin-edit-at-point ()
  "Edit the step or schedule at point."
  (interactive)
  (pcase (get-text-property (line-beginning-position) 'sql-datum-section)
    ("Steps"     (sql-datum-admin-edit-step))
    ("Schedules" (sql-datum-admin-edit-schedule))
    (_ (pcase (cons sql-datum--admin-panel-name (sql-datum--admin-sub-panel))
         ('("databases" . "files") (sql-datum-admin-edit-file))
         (`("databases" . ,_)      (sql-datum-admin-edit-database))
         ('("security" . "user-mappings") (sql-datum-admin-edit-mapping))
         ('("schema" . "tables")   (sql-datum-admin-edit-table))
         (`("security" . ,_)       (sql-datum-admin-edit-principal))
         ;; The job's own properties.  Its steps and schedules are
         ;; edited from the detail view, which the sections above catch.
         (`("jobs" . ,_)           (sql-datum-admin-edit-job))
         (_ (user-error "No editable item at point"))))))

(defun sql-datum-admin-new-at-point ()
  "Create a new item, depending on cursor section or current panel."
  (interactive)
  (pcase (get-text-property (line-beginning-position) 'sql-datum-section)
    ("Steps"     (sql-datum-admin-new-step))
    ("Schedules" (sql-datum-admin-new-schedule))
    (_ (pcase (cons sql-datum--admin-panel-name (sql-datum--admin-sub-panel))
         ('("databases" . "files")       (sql-datum-admin-new-file))
         ('("databases" . "backups")     (sql-datum-admin-new-backup))
         ('("schema" . "tables")         (sql-datum-admin-new-table))
         (`("schema" . ,_)               (sql-datum-admin-new-schema))
         (`("databases" . ,_)            (sql-datum-admin-new-database))
         ('("security" . "user-mappings") (sql-datum-admin-new-mapping))
         (`("security" . ,_)             (sql-datum-admin-new-principal))
         (`("jobs" . ,_)                 (sql-datum-admin-new-job))
         (_ (user-error "No section at point for creating items"))))))

(defun sql-datum-admin-delete-at-point ()
  "Delete the item at point, depending on cursor section or current panel."
  (interactive)
  (pcase (get-text-property (line-beginning-position) 'sql-datum-section)
    ("Steps"     (sql-datum-admin-delete-step))
    ("Schedules" (sql-datum-admin-delete-schedule))
    (_ (pcase (cons sql-datum--admin-panel-name (sql-datum--admin-sub-panel))
         ('("databases" . "files")        (sql-datum-admin-remove-file))
         (`("databases" . ,_)             (sql-datum-admin-drop-database))
         ('("schema" . "tables")          (sql-datum-admin-drop-table))
         (`("schema" . ,_)                (sql-datum-admin-drop-schema))
         ('("security" . "user-mappings") (sql-datum-admin-remove-mapping))
         (`("security" . ,_)              (sql-datum-admin-drop-principal))
         (`("jobs" . ,_)                  (sql-datum-admin-delete-job))
         (_ (user-error "No deletable item at point"))))))

;; --- Database wizard commands ---

(defun sql-datum-admin-new-database ()
  "Open the create-database wizard."
  (interactive)
  (sql-datum--admin-send-command ":admin-action databases new-database"))

(defun sql-datum-admin-edit-database ()
  "Edit the settings of the database at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No database at point"))
    (sql-datum--admin-send-command
     (format ":admin-action databases edit-database %s" name))))

;; --- Database file management ---

(defun sql-datum--admin-sub-panel ()
  "Return the sub-panel name of the current admin buffer, or nil."
  (alist-get 'sub_panel sql-datum--admin-panel-data))

(defun sql-datum--admin-file-payload (&optional extra)
  "Return a base64 payload naming the current database and file at point."
  (let ((database (alist-get 'database sql-datum--admin-context))
        (logical (sql-datum--admin-row-id-at-point)))
    (unless database (user-error "No database context available"))
    (base64-encode-string
     (encode-coding-string
      (json-serialize (append `((database . ,database))
                              (when logical `((logical . ,logical)))
                              extra))
      'utf-8)
     t)))

(defun sql-datum-admin-database-files ()
  "Show the files making up the database at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No database at point"))
    (setq sql-datum--admin-display-request "databases")
    (sql-datum--admin-send-command
     (format ":admin-action databases files %s" name))))

(defun sql-datum-admin-schema-tables ()
  "Show the tables in the schema at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No schema at point"))
    (setq sql-datum--admin-display-request "schema")
    (sql-datum--admin-send-command
     (format ":admin-action schema tables %s" name))))

(defun sql-datum-admin-new-schema ()
  "Create a schema in the current database."
  (interactive)
  (sql-datum--admin-send-command ":admin-action schema new-schema"))

(defun sql-datum-admin-drop-schema ()
  "Drop the schema at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No schema at point"))
    (sql-datum--admin-send-command
     (format ":admin-action schema drop-schema %s" name))))

(defun sql-datum-admin-new-table ()
  "Create a table in the schema whose tables are listed."
  (interactive)
  (let ((schema (alist-get 'schema sql-datum--admin-context)))
    (unless schema (user-error "No schema context available"))
    (sql-datum--admin-send-command
     (format ":admin-action schema new-table %s" schema))))

(defun sql-datum-admin-edit-table ()
  "Alter the columns of the table at point."
  (interactive)
  (let ((schema (alist-get 'schema sql-datum--admin-context))
        (table (sql-datum--admin-row-id-at-point)))
    (unless (and schema table) (user-error "No table at point"))
    (sql-datum--admin-send-command
     (format ":admin-action schema edit-table %s"
             (sql-datum--admin-payload
              `((schema . ,schema) (table . ,table)))))))

(defun sql-datum-admin-drop-table ()
  "Drop the table at point."
  (interactive)
  (let ((schema (alist-get 'schema sql-datum--admin-context))
        (table (sql-datum--admin-row-id-at-point)))
    (unless (and schema table) (user-error "No table at point"))
    (when (yes-or-no-p (format "Drop table %s.%s? " schema table))
      (sql-datum--admin-send-command
       (format ":admin-action schema drop-table %s"
               (base64-encode-string
                (encode-coding-string
                 (json-serialize `((schema . ,schema) (table . ,table)))
                 'utf-8)
                t))))))

(defun sql-datum-admin-database-backups ()
  "Show the backup history of the database at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No database at point"))
    (setq sql-datum--admin-display-request "databases")
    (sql-datum--admin-send-command
     (format ":admin-action databases backups %s" name))))

(defun sql-datum-admin-new-backup ()
  "Back up the database whose backups are listed."
  (interactive)
  (sql-datum--admin-send-command
   (format ":admin-action databases new-backup %s"
           (sql-datum--admin-backup-payload))))

(defun sql-datum-admin-restore ()
  "Restore the database whose backups are listed.
When point is on a recorded backup, its file is offered as the source."
  (interactive)
  (sql-datum--admin-send-command
   (format ":admin-action databases restore %s"
           (sql-datum--admin-backup-payload))))

(defun sql-datum--admin-backup-payload ()
  "Return a payload naming the database and the device at point."
  (let ((database (alist-get 'database sql-datum--admin-context))
        (device (sql-datum--admin-row-id-at-point)))
    (unless database (user-error "No database context available"))
    (base64-encode-string
     (encode-coding-string
      (json-serialize (append `((database . ,database))
                              (when device `((device . ,device)))))
      'utf-8)
     t)))

(defun sql-datum-admin-new-file ()
  "Add a file to the database whose files are listed."
  (interactive)
  (sql-datum--admin-send-command
   (format ":admin-action databases new-file %s"
           (sql-datum--admin-file-payload))))

(defun sql-datum-admin-edit-file ()
  "Edit the size limits of the file at point."
  (interactive)
  (unless (sql-datum--admin-row-id-at-point)
    (user-error "No file at point"))
  (sql-datum--admin-send-command
   (format ":admin-action databases edit-file %s"
           (sql-datum--admin-file-payload))))

(defun sql-datum-admin-remove-file ()
  "Remove the file at point from its database."
  (interactive)
  (let ((logical (sql-datum--admin-row-id-at-point)))
    (unless logical (user-error "No file at point"))
    (when (yes-or-no-p (format "Remove file '%s'? " logical))
      (sql-datum--admin-send-command
       (format ":admin-action databases remove-file %s"
               (sql-datum--admin-file-payload))))))

(defun sql-datum-admin-shrink-file ()
  "Shrink the file at point."
  (interactive)
  (unless (sql-datum--admin-row-id-at-point)
    (user-error "No file at point"))
  (sql-datum--admin-send-command
   (format ":admin-action databases shrink-file %s"
           (sql-datum--admin-file-payload))))

;; --- Security panel (logins, roles, database users) ---

(defun sql-datum-admin-new-principal ()
  "Create a new login or role."
  (interactive)
  (sql-datum--admin-send-command ":admin-action security new-principal"))

(defun sql-datum-admin-edit-principal ()
  "Edit the login or role at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "Nothing at point"))
    (sql-datum--admin-send-command
     (format ":admin-action security edit-principal %s" name))))

(defun sql-datum-admin-drop-principal ()
  "Drop the login or role at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "Nothing at point"))
    (sql-datum--admin-send-command
     (format ":admin-action security drop-check %s" name))))

(defun sql-datum-admin-user-mappings ()
  "Show the databases the login at point is a user in."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No login at point"))
    (setq sql-datum--admin-display-request "security")
    (sql-datum--admin-send-command
     (format ":admin-action security mappings %s" name))))

(defun sql-datum--admin-mapping-payload ()
  "Return a base64 payload naming the login and the database at point."
  (let ((login (alist-get 'login sql-datum--admin-context))
        (database (sql-datum--admin-row-id-at-point)))
    (unless login (user-error "No login context available"))
    (base64-encode-string
     (encode-coding-string
      (json-serialize (append `((login . ,login))
                              (when database `((database . ,database)))))
      'utf-8)
     t)))

(defun sql-datum-admin-new-mapping ()
  "Map the current login into a database."
  (interactive)
  (sql-datum--admin-send-command
   (format ":admin-action security new-mapping %s"
           (sql-datum--admin-mapping-payload))))

(defun sql-datum-admin-edit-mapping ()
  "Edit the database roles held in the database at point."
  (interactive)
  (unless (sql-datum--admin-row-id-at-point)
    (user-error "No database at point"))
  (sql-datum--admin-send-command
   (format ":admin-action security edit-mapping %s"
           (sql-datum--admin-mapping-payload))))

(defun sql-datum-admin-remove-mapping ()
  "Remove the current login's user from the database at point."
  (interactive)
  (let ((database (sql-datum--admin-row-id-at-point))
        (login (alist-get 'login sql-datum--admin-context)))
    (unless database (user-error "No database at point"))
    (when (yes-or-no-p (format "Remove %s from %s? " login database))
      (sql-datum--admin-send-command
       (format ":admin-action security remove-mapping %s"
               (sql-datum--admin-mapping-payload))))))

(defun sql-datum--admin-permission-context ()
  "Return the principal and database the permissions view is about.

From the principal list the row at point names the principal and there
is no database; from a user-mapping row the context carries the login
and the row names the database."
  (pcase (cons sql-datum--admin-panel-name (sql-datum--admin-sub-panel))
    ('("security" . "user-mappings")
     (let ((login (alist-get 'login sql-datum--admin-context))
           (database (sql-datum--admin-row-id-at-point)))
       (unless (and login database) (user-error "No database at point"))
       (list login database)))
    ('("security" . "permissions")
     (list (alist-get 'principal sql-datum--admin-context)
           (let ((db (alist-get 'database sql-datum--admin-context)))
             (and db (not (string-empty-p db)) db))))
    (`("security" . ,_)
     (let ((name (sql-datum--admin-row-id-at-point)))
       (unless name (user-error "No login or role at point"))
       (list name nil)))
    (_ (user-error "Permissions are shown from the security panel"))))

(defun sql-datum-admin-permissions ()
  "Show what the login or role at point may do."
  (interactive)
  (pcase-let ((`(,principal ,database)
               (sql-datum--admin-permission-context)))
    (setq sql-datum--admin-display-request "security")
    (sql-datum--admin-send-command
     (format ":admin-action security permissions %s"
             (sql-datum--admin-payload
              (append `((principal . ,principal))
                      (when database `((database . ,database)))))))))

(defun sql-datum-admin-grant ()
  "Grant or deny a permission to the principal this panel is about."
  (interactive)
  (unless (equal (sql-datum--admin-sub-panel) "permissions")
    (user-error "Granting is done from the permissions view (P)"))
  (pcase-let ((`(,principal ,database)
               (sql-datum--admin-permission-context)))
    (sql-datum--admin-send-command
     (format ":admin-action security new-permission %s"
             (sql-datum--admin-payload
              (append `((principal . ,principal))
                      (when database `((database . ,database)))))))))

(defun sql-datum-admin-revoke-permission ()
  "Revoke the permission on the row at point."
  (interactive)
  (let ((cells (sql-datum--admin-row-cells-at-point)))
    (unless cells (user-error "No permission at point"))
    (pcase-let ((`(,principal ,database)
                 (sql-datum--admin-permission-context)))
      (let ((state (nth 0 cells))
            (permission (nth 1 cells))
            (scope (nth 2 cells))
            (securable (nth 3 cells))
            (column (or (nth 4 cells) "")))
        (when (yes-or-no-p
               (format "Revoke %s %s on %s%s from %s? "
                       state permission securable
                       (if (string-empty-p column) ""
                         (format " (%s)" column))
                       principal))
          (sql-datum--admin-send-command
           (format ":admin-action security revoke-permission %s"
                   (sql-datum--admin-payload
                    (append `((principal . ,principal)
                              (permission . ,permission)
                              (scope . ,scope)
                              (securable . ,securable)
                              (column . ,column))
                            (when database `((database . ,database)))))))))))) 

(defun sql-datum-admin-enable-or-open ()
  "Open the entry at point, or toggle the job at point.

`e\=' is dired's key for opening what point is on, which is what it
means in the filesystem panel; everywhere else it keeps its meaning of
enabling or disabling a job."
  (interactive)
  (if (equal sql-datum--admin-panel-name "filesystem")
      (sql-datum-admin-open-path)
    (sql-datum-admin-toggle-enable)))

(defun sql-datum-admin-toggle-details ()
  "Show only names in the listing, or show the details again.
`(\=' is dired's key for this."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  (setq sql-datum--admin-hide-details (not sql-datum--admin-hide-details))
  (sql-datum--admin-show-panel sql-datum--admin-panel-data
                               sql-datum--admin-sqli-buf)
  (message "datum: %s"
           (if sql-datum--admin-hide-details "names only" "details shown")))

(defconst sql-datum--admin-fs-sort-cycle '(1 2 3)
  "Columns `s\=' cycles through in a filesystem listing: name, size, date.")

(defun sql-datum-admin-cycle-sort (&optional reverse)
  "Sort the listing by the next column in turn.

`s\=' cycles the sort in dired too, between name and date; here it also
takes in size, which this listing has a column for.  With a prefix
argument, REVERSE turns the current sort around instead of moving on,
so each of the three can be read either way."
  (interactive "P")
  (let* ((cycle sql-datum--admin-fs-sort-cycle)
         (current sql-datum--admin-sort-column)
         (headers (alist-get 'headers sql-datum--admin-panel-data))
         (column (cond
                  ;; Reversing an unsorted listing has to sort it first.
                  ((and reverse (member current cycle)) current)
                  ((member current cycle)
                   (or (nth (1+ (cl-position current cycle)) cycle)
                       (car cycle)))
                  (t (car cycle)))))
    (setq sql-datum--admin-sort-ascending
          (if (and reverse (eql column current))
              (not sql-datum--admin-sort-ascending)
            t)
          sql-datum--admin-sort-column column)
    (sql-datum--admin-show-panel sql-datum--admin-panel-data
                                 sql-datum--admin-sqli-buf)
    (message "datum: sorted by %s, %s"
             (or (nth column headers) column)
             (if sql-datum--admin-sort-ascending "ascending" "descending"))))

(defun sql-datum-admin-start-or-sort (&optional reverse)
  "Cycle the sort in a directory listing, or start the job at point.
In a listing, a prefix argument REVERSEs the current sort rather than
moving to the next column."
  (interactive "P")
  (if (equal sql-datum--admin-panel-name "filesystem")
      (sql-datum-admin-cycle-sort reverse)
    (sql-datum-admin-start-job)))

(defun sql-datum--admin-handle-downloaded (data sqli-buf)
  "Note that a file arrived, and open it if that is what was asked.
DATA is the panel the server sent; SQLI-BUF is the connection."
  (let ((local (alist-get 'local data))
        (remote (alist-get 'remote data))
        (failures (alist-get 'rows data)))
    (when (and remote local (file-readable-p local)
               (not (file-directory-p local)))
      (sql-datum--fs-cache-put remote local
                               (alist-get 'size data)
                               (alist-get 'modified data)
                               sqli-buf))
    ;; Where a copy was wanted for keeping, the temporary one is moved
    ;; on rather than the file being fetched twice.
    (let ((final (alist-get 'final data)))
      (when (and final (not (string-empty-p final)) local
                 (file-readable-p local))
        (condition-case err
            (progn
              (make-directory (file-name-directory final) t)
              (copy-file local final t t)
              (message "datum: copied to %s" final))
          (error (message "datum: could not put it at %s: %s"
                          final (error-message-string err))))))
    (cond
     ((and (equal (alist-get 'then data) "open") local
           (file-readable-p local))
      ;; Emacs decides what it is: a zip opens in `archive-mode', an
      ;; image in `image-mode', and so on.
      (find-file local))
     (failures
      (message "datum: %s (%d could not be read)"
               (alist-get 'info data) (length failures)))
     (t (message "datum: %s" (alist-get 'info data))))))

(defun sql-datum--admin-fs-apply-marks ()
  "Show which rows are marked, the way dired shows it."
  (when sql-datum--admin-fs-marks
    (require 'dired nil t)
    (setq left-margin-width 2)
    (save-excursion
      (goto-char (point-min))
      (while (not (eobp))
        (let ((id (sql-datum--admin-row-id-at-point)))
          (when (and id (member id sql-datum--admin-fs-marks))
            (let ((overlay (make-overlay (line-beginning-position)
                                         (line-end-position))))
              (overlay-put overlay 'sql-datum-mark t)
              (overlay-put overlay 'face 'dired-marked)
              ;; In the margin, so marking cannot shift the columns.
              (overlay-put overlay 'before-string
                           (propertize "*" 'display
                                       '((margin left-margin) "*"))))))
        (forward-line 1)))))

(defun sql-datum--admin-fs-targets ()
  "Return the rows to act on: the marked ones, or the one at point.
Each is a cons of the type and the path."
  (if sql-datum--admin-fs-marks
      (let (found)
        (save-excursion
          (goto-char (point-min))
          (while (not (eobp))
            (let ((id (sql-datum--admin-row-id-at-point)))
              (when (and id (member id sql-datum--admin-fs-marks))
                (let ((cells (sql-datum--admin-row-cells-at-point)))
                  (push (list (if cells (nth 0 cells) "file") id
                              (and cells (nth 1 cells))
                              (and cells (nth 2 cells))
                              (and cells (nth 3 cells)))
                        found))))
            (forward-line 1)))
        (nreverse found))
    (let ((cells (sql-datum--admin-row-cells-at-point))
          (path (sql-datum--admin-row-id-at-point)))
      (unless path (user-error "Nothing at point"))
      (list (list (if cells (nth 0 cells) "file") path
                  (and cells (nth 1 cells))
                  (and cells (nth 2 cells))
                  (and cells (nth 3 cells)))))))

(defun sql-datum-admin-fs-mark ()
  "Mark the entry at point and move on, as `m\=' does in dired."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  (let ((path (sql-datum--admin-row-id-at-point))
        (cells (sql-datum--admin-row-cells-at-point)))
    (unless path (user-error "Nothing at point"))
    (when (equal (and cells (nth 1 cells)) "..")
      (user-error "The way up is not something to mark"))
    (cl-pushnew path sql-datum--admin-fs-marks :test #'equal)
    (sql-datum--admin-fs-apply-marks)
    (forward-line 1)
    (message "datum: %d marked" (length sql-datum--admin-fs-marks))))

(defun sql-datum-admin-fs-unmark ()
  "Unmark the entry at point and move on, as `u\=' does in dired."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  (let ((path (sql-datum--admin-row-id-at-point)))
    (setq sql-datum--admin-fs-marks
          (delete path sql-datum--admin-fs-marks))
    (remove-overlays (line-beginning-position) (line-end-position)
                     'sql-datum-mark t)
    (forward-line 1)
    (message "datum: %d marked" (length sql-datum--admin-fs-marks))))

(defun sql-datum-admin-fs-unmark-all ()
  "Drop every mark, as `U\=' does in dired."
  (interactive)
  (setq sql-datum--admin-fs-marks nil)
  (remove-overlays (point-min) (point-max) 'sql-datum-mark t)
  (message "datum: no marks"))

(defun sql-datum-admin-fs-toggle-marks ()
  "Mark what is not marked and unmark what is, as `t\=' does in dired."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  (let (marks)
    (save-excursion
      (goto-char (point-min))
      (while (not (eobp))
        (let ((id (sql-datum--admin-row-id-at-point))
              (cells (sql-datum--admin-row-cells-at-point)))
          (when (and id (not (equal (and cells (nth 1 cells)) ".."))
                     (not (member id sql-datum--admin-fs-marks)))
            (push id marks)))
        (forward-line 1)))
    (setq sql-datum--admin-fs-marks (nreverse marks))
    (remove-overlays (point-min) (point-max) 'sql-datum-mark t)
    (sql-datum--admin-fs-apply-marks)
    (message "datum: %d marked" (length sql-datum--admin-fs-marks))))

(defun sql-datum-admin-open-locally ()
  "Copy the file at point to this machine and let Emacs open it.

`v\=' shows a file as text, which is what a log wants.  This is for
everything else: the file is fetched whole, so Emacs opens it as
whatever it is — a zip in `archive-mode\=', an image in `image-mode\='.
A copy already fetched and still current is reused."
  (interactive)
  (pcase-let ((`(,type ,path) (sql-datum--admin-fs-row)))
    (when (equal type "dir")
      (user-error "%s is a directory — RET opens it" path))
    (let* ((cells (sql-datum--admin-row-cells-at-point))
           (size (nth 2 cells))
           (modified (nth 3 cells))
           (cached (sql-datum--fs-cached path size modified)))
      (if cached
          (progn (find-file cached)
                 (message "datum: already had it, at %s" cached))
        (message "datum: fetching %s..." path)
        (sql-datum--admin-send-command
         (format ":admin-action filesystem download %s"
                 (sql-datum--admin-payload
                  `((path . ,path)
                    (local . ,(sql-datum--fs-local-name path))
                    (then . "open")
                    (limit . ,sql-datum-download-limit)))))))))

(defun sql-datum-admin-open-or-sort ()
  "Open the file at point locally, or ask which column to sort by.
`o\=' opens in dired; elsewhere in the panels it has always prompted for
a sort column, and still does."
  (interactive)
  (if (equal sql-datum--admin-panel-name "filesystem")
      (sql-datum-admin-open-locally)
    (sql-datum-admin-sort)))

(defun sql-datum-admin-unmark-or-mappings ()
  "Drop every mark, or show the database users of the login at point.
`U\=' unmarks in dired; in the security panel it keeps its own meaning."
  (interactive)
  (if (equal sql-datum--admin-panel-name "filesystem")
      (sql-datum-admin-fs-unmark-all)
    (sql-datum-admin-user-mappings)))

(defun sql-datum-admin-fs-copy (destination)
  "Copy what is marked, or what is at point, into DESTINATION.

A directory is copied with everything under it.  A file already
fetched and still current is copied from that copy rather than being
pulled again, which is the point of keeping it."
  (interactive
   (progn
     (unless (equal sql-datum--admin-panel-name "filesystem")
       (user-error "Not in the server filesystem panel"))
     (list (read-directory-name "Copy to: " default-directory nil nil))))
  (let ((targets (sql-datum--admin-fs-targets))
        (fetched 0) (reused 0) (trees 0))
    (dolist (target targets)
      (pcase-let ((`(,type ,path ,name ,size ,modified) target))
        (let* ((base (or (and name (not (string-empty-p name)) name)
                         (file-name-nondirectory path)))
               (final (expand-file-name base destination)))
          (cond
           ((equal type "dir")
            (cl-incf trees)
            (sql-datum--admin-send-command
             (format ":admin-action filesystem download-tree %s"
                     (sql-datum--admin-payload
                      `((path . ,path) (local . ,final)
                        (limit . ,sql-datum-download-limit))))))
           ((sql-datum--fs-cached path size modified)
            (cl-incf reused)
            (make-directory destination t)
            (copy-file (sql-datum--fs-cached path size modified) final t t))
           (t
            (cl-incf fetched)
            ;; Fetched into the cache and put in place from there, so
            ;; looking at it again later costs nothing.
            (sql-datum--admin-send-command
             (format ":admin-action filesystem download %s"
                     (sql-datum--admin-payload
                      `((path . ,path)
                        (local . ,(sql-datum--fs-local-name path))
                        (final . ,final)
                        (limit . ,sql-datum-download-limit))))))))))
    (message "datum: %s"
             (string-join
              (delq nil
                    (list (and (> reused 0)
                               (format "%d already had" reused))
                          (and (> fetched 0) (format "%d fetching" fetched))
                          (and (> trees 0)
                               (format "%d director%s" trees
                                       (if (= trees 1) "y" "ies")))))
              ", "))))

(defun sql-datum-admin-restore-or-revoke ()
  "Revoke the permission at point, or restore the backup at point.
`R\=' means restore in the backups view and revoke in the permissions one."
  (interactive)
  (if (equal (sql-datum--admin-sub-panel) "permissions")
      (sql-datum-admin-revoke-permission)
    (sql-datum-admin-restore)))

(defun sql-datum--admin-fs-row ()
  "Return (TYPE PATH) for the filesystem row at point."
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  (let ((cells (sql-datum--admin-row-cells-at-point))
        (path (sql-datum--admin-row-id-at-point)))
    (unless path (user-error "No file or directory at point"))
    (list (if cells (nth 0 cells) "file") path)))

(defun sql-datum-admin-open-path ()
  "Open the directory at point, or view the file at point."
  (interactive)
  (pcase-let ((`(,type ,path) (sql-datum--admin-fs-row)))
    (if (equal type "dir")
        (progn
          (setq sql-datum--admin-display-request "filesystem")
          (sql-datum--admin-send-command
           (format ":admin filesystem %s" path)))
      (sql-datum-admin-view-file))))

(defun sql-datum-admin-parent-directory ()
  "Go up one directory in the server filesystem panel."
  (interactive)
  (unless (equal sql-datum--admin-panel-name "filesystem")
    (user-error "Not in the server filesystem panel"))
  ;; The panel lists the parent as its first row, so this is the same
  ;; path the server already worked out rather than one guessed here.
  (let ((parent (save-excursion
                  (goto-char (point-min))
                  (catch 'found
                    (while (not (eobp))
                      (let ((cells (sql-datum--admin-row-cells-at-point)))
                        (when (and cells (equal (nth 1 cells) ".."))
                          (throw 'found (sql-datum--admin-row-id-at-point))))
                      (forward-line 1))
                    nil))))
    (unless parent (user-error "Already at the top"))
    (setq sql-datum--admin-display-request "filesystem")
    (sql-datum--admin-send-command
     (format ":admin filesystem %s" parent))))

(defun sql-datum-admin-view-file ()
  "View the contents of the file at point."
  (interactive)
  (pcase-let ((`(,type ,path) (sql-datum--admin-fs-row)))
    (when (equal type "dir")
      (user-error "%s is a directory" path))
    (sql-datum--admin-send-command
     (format ":admin-action filesystem view %s"
             (sql-datum--admin-payload `((path . ,path)))))))

(defun sql-datum-admin-copy-path ()
  "Copy the path at point to the kill ring."
  (interactive)
  (pcase-let ((`(,_type ,path) (sql-datum--admin-fs-row)))
    (kill-new path)
    (message "datum: %s" path)))

(defun sql-datum-admin-stop-or-shrink ()
  "Shrink the file at point, or stop the job at point.
`S' means stop in the jobs panel and shrink in the database files view."
  (interactive)
  (if (and (equal sql-datum--admin-panel-name "databases")
           (equal (sql-datum--admin-sub-panel) "files"))
      (sql-datum-admin-shrink-file)
    (sql-datum-admin-stop-job)))

(defun sql-datum-admin-drop-database ()
  "Open the drop-database confirmation for the database at point."
  (interactive)
  (let ((name (sql-datum--admin-row-id-at-point)))
    (unless name (user-error "No database at point"))
    (sql-datum--admin-send-command
     (format ":admin-action databases drop-check %s" name))))

;; --- Step action commands ---

(defun sql-datum-admin-edit-step ()
  "Edit the step at point in a job detail buffer."
  (interactive)
  (let ((step-id (sql-datum--admin-row-id-at-point))
        (job-name (alist-get 'job_name sql-datum--admin-context)))
    (unless step-id (user-error "No step at point"))
    (unless job-name (user-error "No job context available"))
    (sql-datum--admin-send-command
      (format ":admin-action jobs edit-step %s %s"
              step-id job-name))))

(defun sql-datum-admin-new-step ()
  "Create a new step for the current job."
  (interactive)
  (let ((job-name (alist-get 'job_name sql-datum--admin-context)))
    (unless job-name (user-error "No job context available"))
    ;; Open a blank step editor locally
    (sql-datum--admin-show-step-editor
     `((panel . "jobs")
       (sub_panel . "step-edit")
       (title . ,(format "New Step for: %s" job-name))
       (step . ((step_id . nil)
                (step_name . "")
                (subsystem . "TSQL")
                (command . "")
                (database_name . "")
                (retry_attempts . 0)
                (retry_interval . 0)
                (on_success_action . 3)
                (on_success_step_id . 0)
                (on_fail_action . 2)
                (on_fail_step_id . 0)))
       (subsystems . (("TSQL" . "T-SQL") ("CmdExec" . "OS Command")
                      ("PowerShell" . "PowerShell") ("SSIS" . "SSIS")))
       (step_actions . ((1 . "Quit with success") (2 . "Quit with failure")
                        (3 . "Go to next step") (4 . "Go to step...")))
       (context . ((job_name . ,job-name))))
     sql-datum--admin-sqli-buf)))

(defun sql-datum-admin-delete-step ()
  "Delete the step at point."
  (interactive)
  (let ((step-id (sql-datum--admin-row-id-at-point))
        (job-name (alist-get 'job_name sql-datum--admin-context)))
    (unless step-id (user-error "No step at point"))
    (unless job-name (user-error "No job context available"))
    (when (yes-or-no-p (format "Delete step %s? " step-id))
      (sql-datum--admin-send-command
        (format ":admin-action jobs delete-step %s %s" step-id job-name)))))

;; --- Schedule action commands ---

(defun sql-datum-admin-edit-schedule ()
  "Edit the schedule at point in a job detail buffer."
  (interactive)
  (let ((sched-name (sql-datum--admin-row-id-at-point))
        (job-name (alist-get 'job_name sql-datum--admin-context)))
    (unless sched-name (user-error "No schedule at point"))
    (unless job-name (user-error "No job context available"))
    (sql-datum--admin-send-command
     (format ":admin-action jobs edit-schedule %s %s"
             sched-name job-name))))

(defun sql-datum-admin-new-schedule ()
  "Create a new schedule for the current job."
  (interactive)
  (let ((job-name (alist-get 'job_name sql-datum--admin-context)))
    (unless job-name (user-error "No job context available"))
    ;; Open a blank schedule editor
    (sql-datum--admin-show-schedule-editor
     `((panel . "jobs")
       (sub_panel . "schedule-edit")
       (title . ,(format "New Schedule for: %s" job-name))
       (schedule . ((schedule_id . nil)
                    (name . "")
                    (enabled . 1)
                    (freq_type . 4)
                    (freq_interval . 1)
                    (freq_subday_type . 1)
                    (freq_subday_interval . 0)
                    (freq_relative_interval . 0)
                    (freq_recurrence_factor . 0)
                    (active_start_date . 20000101)
                    (active_end_date . 99991231)
                    (active_start_time . 0)
                    (active_end_time . 235959)))
       (freq_types . ((1 . "Once") (4 . "Daily") (8 . "Weekly")
                      (16 . "Monthly") (32 . "Monthly Relative")
                      (64 . "On Agent Start") (128 . "On Idle")))
       (subday_types . ((1 . "At specified time") (2 . "Seconds")
                        (4 . "Minutes") (8 . "Hours")))
       (context . ((job_name . ,job-name))))
     sql-datum--admin-sqli-buf)))

(defun sql-datum-admin-delete-schedule ()
  "Delete the schedule at point."
  (interactive)
  (let ((sched-name (sql-datum--admin-row-id-at-point)))
    (unless sched-name (user-error "No schedule at point"))
    (when (yes-or-no-p (format "Delete schedule '%s'? " sched-name))
      (sql-datum--admin-send-command
       (format ":admin-action jobs delete-schedule %s" sched-name)))))

;; --- Schedule editor (widget-based form) ---

(defun sql-datum--admin-show-schedule-editor (data sqli-buf)
  "Show a widget-based schedule editor from DATA.
SQLI-BUF is the originating SQLi buffer."
  (require 'widget)
  (require 'wid-edit)
  (let* ((schedule (alist-get 'schedule data))
         (freq-types (alist-get 'freq_types data))
         (subday-types (alist-get 'subday_types data))
         (context (alist-get 'context data))
         (job-name (alist-get 'job_name context))
         (title (or (alist-get 'title data) "Schedule Editor"))
         (is-new (null (alist-get 'schedule_id schedule)))
         (buf (get-buffer-create "*datum-admin:schedule-edit*")))
    (with-current-buffer buf
      (kill-all-local-variables)
      (let ((inhibit-read-only t))
        (erase-buffer))
      (remove-overlays)
      (widget-insert (propertize title 'face 'bold))
      (widget-insert "\n\n")
      ;; Store widgets for later retrieval
      (let (widgets)
        ;; Name
        (widget-insert "Schedule Name: ")
        (push (cons 'name (widget-create 'editable-field
                                         :size 40
                                         :value (or (alist-get 'name schedule) "")))
              widgets)
        (widget-insert "\n")
        ;; Enabled
        (widget-insert "Enabled:       ")
        (push (cons 'enabled (widget-create 'checkbox
                                            :value (eq (alist-get 'enabled schedule) 1)))
              widgets)
        (widget-insert "\n\n")
        ;; Frequency type
        (widget-insert "Frequency:     ")
        (let* ((freq-val (or (alist-get 'freq_type schedule) 4))
               (freq-choices (or (mapcar (lambda (ft)
                                           (list 'item
                                                 :tag (cdr ft)
                                                 :value (car ft)))
                                         (if (listp freq-types)
                                             freq-types
                                           '((4 . "Daily"))))
                                 '((item :tag "Daily" :value 4)))))
          (push (cons 'freq_type
                      (apply #'widget-create 'menu-choice
                             :value freq-val
                             freq-choices))
                widgets))
        (widget-insert "\n")
        ;; Frequency interval
        (widget-insert "Interval:      ")
        (push (cons 'freq_interval
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'freq_interval schedule) 1))))
              widgets)
        (widget-insert "\n")
        ;; Subday type
        (widget-insert "Subday Type:   ")
        (let* ((subday-val (or (alist-get 'freq_subday_type schedule) 1))
               (subday-choices (or (mapcar (lambda (st)
                                             (list 'item
                                                   :tag (cdr st)
                                                   :value (car st)))
                                           (if (listp subday-types)
                                               subday-types
                                             '((1 . "At specified time"))))
                                   '((item :tag "At specified time" :value 1)))))
          (push (cons 'freq_subday_type
                      (apply #'widget-create 'menu-choice
                             :value subday-val
                             subday-choices))
                widgets))
        (widget-insert "\n")
        ;; Subday interval
        (widget-insert "Subday Int.:   ")
        (push (cons 'freq_subday_interval
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'freq_subday_interval schedule) 0))))
              widgets)
        (widget-insert "\n\n")
        ;; Active times
        (widget-insert "Start Time:    ")
        (push (cons 'active_start_time
                    (widget-create 'editable-field
                                   :size 10
                                   :value (sql-datum--admin-format-time
                                           (or (alist-get 'active_start_time schedule) 0))))
              widgets)
        (widget-insert "  (HHMMSS)\n")
        (widget-insert "End Time:      ")
        (push (cons 'active_end_time
                    (widget-create 'editable-field
                                   :size 10
                                   :value (sql-datum--admin-format-time
                                           (or (alist-get 'active_end_time schedule) 235959))))
              widgets)
        (widget-insert "  (HHMMSS)\n")
        ;; Active dates
        (widget-insert "Start Date:    ")
        (push (cons 'active_start_date
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'active_start_date schedule) 20000101))))
              widgets)
        (widget-insert "  (YYYYMMDD)\n")
        (widget-insert "End Date:      ")
        (push (cons 'active_end_date
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'active_end_date schedule) 99991231))))
              widgets)
        (widget-insert "  (YYYYMMDD)\n\n")
        ;; Buttons
        (widget-create 'push-button
                       :notify (lambda (&rest _)
                                 (sql-datum--admin-schedule-submit
                                  widgets schedule sqli-buf job-name is-new))
                       (if is-new "Create Schedule" "Update Schedule"))
        (widget-insert "  ")
        (widget-create 'push-button
                       :notify (lambda (&rest _) (quit-window t))
                       "Cancel")
        (widget-insert "\n")
        ;; Store widgets for submit handler
        (setq-local sql-datum--schedule-widgets widgets)
        (setq-local sql-datum--admin-sqli-buf sqli-buf))
      (use-local-map (sql-datum--widget-keymap))
      (widget-setup)
      (goto-char (point-min)))
    (switch-to-buffer buf)))

(defun sql-datum--admin-format-time (time-int)
  "Format TIME-INT (integer HHMMSS) as a string."
  (format "%06d" (if (numberp time-int) time-int 0)))

(defun sql-datum--admin-schedule-submit (widgets schedule sqli-buf job-name is-new)
  "Submit the schedule form with WIDGETS data.
SCHEDULE is the original schedule data, SQLI-BUF the connection buffer,
JOB-NAME the parent job, IS-NEW non-nil for creating a new schedule."
  (let* ((get-val (lambda (key)
                    (let ((w (alist-get key widgets)))
                      (when w (widget-value w)))))
         (data `((name . ,(string-trim (funcall get-val 'name)))
                 (enabled . ,(if (funcall get-val 'enabled) 1 0))
                 (freq_type . ,(funcall get-val 'freq_type))
                 (freq_interval . ,(string-to-number
                                    (funcall get-val 'freq_interval)))
                 (freq_subday_type . ,(funcall get-val 'freq_subday_type))
                 (freq_subday_interval . ,(string-to-number
                                           (funcall get-val 'freq_subday_interval)))
                 (freq_relative_interval . ,(or (alist-get 'freq_relative_interval schedule) 0))
                 (freq_recurrence_factor . ,(or (alist-get 'freq_recurrence_factor schedule) 0))
                 (active_start_date . ,(string-to-number
                                        (funcall get-val 'active_start_date)))
                 (active_end_date . ,(string-to-number
                                      (funcall get-val 'active_end_date)))
                 (active_start_time . ,(string-to-number
                                        (funcall get-val 'active_start_time)))
                 (active_end_time . ,(string-to-number
                                      (funcall get-val 'active_end_time))))))
    ;; Validate
    (when (string-empty-p (alist-get 'name data))
      (user-error "Schedule name cannot be empty"))
    (let ((json-str (json-serialize data)))
      (if is-new
          (sql-datum--admin-send-command-to
           sqli-buf
           (format ":admin-action jobs new-schedule %s %s" job-name json-str))
        ;; Include schedule_id for update
        (push (cons 'schedule_id (alist-get 'schedule_id schedule)) data)
        (let ((json-str-with-id (json-serialize data)))
          (sql-datum--admin-send-command-to
           sqli-buf
           (format ":admin-action jobs update-schedule %s" json-str-with-id)))))
    (quit-window t)
    (message "datum admin: schedule %s" (if is-new "created" "updated"))))

;; --- Step editor (widget-based form) ---

(defun sql-datum--admin-show-step-editor (data sqli-buf)
  "Show a widget-based step editor from DATA.
SQLI-BUF is the originating SQLi buffer."
  (require 'widget)
  (require 'wid-edit)
  (let* ((step (alist-get 'step data))
         (subsystems (alist-get 'subsystems data))
         (step-actions (alist-get 'step_actions data))
         (context (alist-get 'context data))
         (job-name (alist-get 'job_name context))
         (title (or (alist-get 'title data) "Step Editor"))
         (is-new (null (alist-get 'step_id step)))
         (buf (get-buffer-create "*datum-admin:step-edit*")))
    (with-current-buffer buf
      (kill-all-local-variables)
      (let ((inhibit-read-only t))
        (erase-buffer))
      (remove-overlays)
      (widget-insert (propertize title 'face 'bold))
      (widget-insert "\n\n")
      (let (widgets)
        ;; Step Name
        (widget-insert "Step Name:       ")
        (push (cons 'step_name (widget-create 'editable-field
                                              :size 40
                                              :value (or (alist-get 'step_name step) "")))
              widgets)
        (widget-insert "\n")
        ;; Subsystem
        (widget-insert "Subsystem:       ")
        (let* ((sub-val (or (alist-get 'subsystem step) "TSQL"))
               (sub-choices (or (mapcar (lambda (s)
                                          (list 'item
                                                :tag (cdr s)
                                                :value (car s)))
                                        (if (listp subsystems)
                                            subsystems
                                          '(("TSQL" . "T-SQL"))))
                                '((item :tag "T-SQL" :value "TSQL")))))
          (push (cons 'subsystem
                      (apply #'widget-create 'menu-choice
                             :value sub-val
                             sub-choices))
                widgets))
        (widget-insert "\n")
        ;; Database
        (widget-insert "Database:        ")
        (push (cons 'database_name (widget-create 'editable-field
                                                   :size 40
                                                   :value (or (alist-get 'database_name step) "")))
              widgets)
        (widget-insert "\n\n")
        ;; Command (multi-line text)
        (widget-insert "Command:\n")
        (push (cons 'command (widget-create 'text
                                            :size 80
                                            :value (or (alist-get 'command step) "")))
              widgets)
        (widget-insert "\n")
        ;; Retry settings
        (widget-insert "Retry Attempts:  ")
        (push (cons 'retry_attempts
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'retry_attempts step) 0))))
              widgets)
        (widget-insert "\n")
        (widget-insert "Retry Interval:  ")
        (push (cons 'retry_interval
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'retry_interval step) 0))))
              widgets)
        (widget-insert "  (minutes)\n\n")
        ;; On Success Action
        (widget-insert "On Success:      ")
        (let* ((act-val (or (alist-get 'on_success_action step) 3))
               (act-choices (or (mapcar (lambda (a)
                                          (list 'item
                                                :tag (cdr a)
                                                :value (car a)))
                                        (if (listp step-actions)
                                            step-actions
                                          '((3 . "Go to next step"))))
                                '((item :tag "Go to next step" :value 3)))))
          (push (cons 'on_success_action
                      (apply #'widget-create 'menu-choice
                             :value act-val
                             act-choices))
                widgets))
        (widget-insert "\n")
        (widget-insert "On Success Step: ")
        (push (cons 'on_success_step_id
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'on_success_step_id step) 0))))
              widgets)
        (widget-insert "  (for \"Go to step...\")\n")
        ;; On Failure Action
        (widget-insert "On Failure:      ")
        (let* ((act-val (or (alist-get 'on_fail_action step) 2))
               (act-choices (or (mapcar (lambda (a)
                                          (list 'item
                                                :tag (cdr a)
                                                :value (car a)))
                                        (if (listp step-actions)
                                            step-actions
                                          '((2 . "Quit with failure"))))
                                '((item :tag "Quit with failure" :value 2)))))
          (push (cons 'on_fail_action
                      (apply #'widget-create 'menu-choice
                             :value act-val
                             act-choices))
                widgets))
        (widget-insert "\n")
        (widget-insert "On Failure Step: ")
        (push (cons 'on_fail_step_id
                    (widget-create 'editable-field
                                   :size 10
                                   :value (format "%s" (or (alist-get 'on_fail_step_id step) 0))))
              widgets)
        (widget-insert "  (for \"Go to step...\")\n\n")
        ;; Buttons
        (widget-create 'push-button
                       :notify (lambda (&rest _)
                                 (sql-datum--admin-step-submit
                                  widgets step sqli-buf job-name is-new))
                       (if is-new "Create Step" "Update Step"))
        (widget-insert "  ")
        (widget-create 'push-button
                       :notify (lambda (&rest _) (quit-window t))
                       "Cancel")
        (widget-insert "\n")
        (setq-local sql-datum--step-widgets widgets)
        (setq-local sql-datum--admin-sqli-buf sqli-buf))
      (use-local-map (sql-datum--widget-keymap))
      (widget-setup)
      (goto-char (point-min)))
    (switch-to-buffer buf)))

(defun sql-datum--admin-step-submit (widgets step sqli-buf job-name is-new)
  "Submit the step form with WIDGETS data.
STEP is the original step data, SQLI-BUF the connection buffer,
JOB-NAME the parent job, IS-NEW non-nil for creating a new step."
  (let* ((get-val (lambda (key)
                    (let ((w (alist-get key widgets)))
                      (when w (widget-value w)))))
         (data `((step_name . ,(string-trim (funcall get-val 'step_name)))
                 (subsystem . ,(funcall get-val 'subsystem))
                 (command . ,(funcall get-val 'command))
                 (database_name . ,(string-trim (funcall get-val 'database_name)))
                 (retry_attempts . ,(string-to-number
                                     (funcall get-val 'retry_attempts)))
                 (retry_interval . ,(string-to-number
                                     (funcall get-val 'retry_interval)))
                 (on_success_action . ,(funcall get-val 'on_success_action))
                 (on_success_step_id . ,(string-to-number
                                         (funcall get-val 'on_success_step_id)))
                 (on_fail_action . ,(funcall get-val 'on_fail_action))
                 (on_fail_step_id . ,(string-to-number
                                      (funcall get-val 'on_fail_step_id))))))
    ;; Validate
    (when (string-empty-p (alist-get 'step_name data))
      (user-error "Step name cannot be empty"))
    (if is-new
        (let ((json-str (json-serialize data)))
          (sql-datum--admin-send-command-to
           sqli-buf
           (format ":admin-action jobs new-step %s %s" job-name json-str)))
      ;; Include step_id for update
      (push (cons 'step_id (alist-get 'step_id step)) data)
      (let ((json-str (json-serialize data)))
        (sql-datum--admin-send-command-to
         sqli-buf
         (format ":admin-action jobs update-step %s %s" job-name json-str)))))
  (quit-window t)
  (message "datum admin: step %s" (if is-new "created" "updated")))

;; --- Generic wizard form (widget-based, driven by field descriptors) ---
;;
;; The Python side sends a `form' alist describing the fields; this
;; renders them and submits the collected values back as JSON.  All the
;; admin wizards (databases, security, backup, schema) share this, so a
;; new wizard needs no new Emacs code — only new field descriptors.

(defvar-local sql-datum--form-widgets nil
  "Alist of (KEY . WIDGET) for the wizard form in this buffer.")

(defvar-local sql-datum--form-submit-fn nil
  "Zero-argument function submitting the wizard form in this buffer.")

(defvar-local sql-datum--form-browse-fn nil
  "Function of one field key, opening the server path browser for it.")

(defun sql-datum--form-path-fields ()
  "Return the (KEY . LABEL) pairs of this form's path fields."
  (let (found)
    (dolist (entry sql-datum--form-widgets)
      (when (equal (alist-get 'type (cadr entry)) "path")
        (push (cons (car entry)
                    (or (alist-get 'label (cadr entry)) (car entry)))
              found)))
    found))

(defun sql-datum-form-browse ()
  "Browse the server for a path field, without walking to its button.
Uses the field at point when there is one, the only path field when the
form has just one, and otherwise asks which."
  (interactive)
  (unless sql-datum--form-browse-fn
    (user-error "Not in a datum wizard form"))
  (let* ((fields (sql-datum--form-path-fields))
         (at-point (let ((w (widget-field-at (point))))
                     (car (cl-find-if
                           (lambda (entry)
                             (and (equal (alist-get 'type (cadr entry)) "path")
                                  (eq (cddr entry) w)))
                           sql-datum--form-widgets))))
         (key (cond
               (at-point at-point)
               ((null fields) (user-error "This form has no path fields"))
               ((null (cdr fields)) (caar fields))
               (t (let* ((labels (mapcar #'cdr fields))
                         (chosen (completing-read "Browse for: " labels nil t)))
                    (car (rassoc chosen fields)))))))
    (funcall sql-datum--form-browse-fn key)))

(defun sql-datum-form-submit ()
  "Submit the wizard form in the current buffer."
  (interactive)
  (unless sql-datum--form-submit-fn
    (user-error "Not in a datum wizard form"))
  (funcall sql-datum--form-submit-fn))

(defun sql-datum-form-cancel ()
  "Close the wizard form without submitting."
  (interactive)
  (quit-window t))

(defun sql-datum--form-completion-at-point ()
  "Return what TAB should do with the field at point.

The car says which, and the cdr carries what it needs:

  (expand . TEXT)   extend the field to TEXT
  (show . MATCHES)  the value is not a whole candidate yet — offer
                    MATCHES, the way NVARCHAR( still needs a size
  nil               nothing to complete; move to the next field

Matching folds case, so typing In finds INT, and int is rewritten to
INT rather than left as typed.  A value that is already a candidate
returns nil even when longer candidates begin with it, so INT does not
trap point in the cell it shares with INT IDENTITY(1,1)."
  (let ((field (widget-field-at (point))))
    (when field
      (let* ((candidates (widget-get field :completions))
             (text (and (stringp (widget-apply field :value-get))
                        (widget-apply field :value-get)))
             (completion-ignore-case t))
        (when (and candidates text (not (string-empty-p text)))
          (let ((expansion (try-completion text candidates)))
            (cond
             ;; A size the suggestions do not name, VARCHAR(120): the
             ;; field is the user's to keep.
             ((null expansion) nil)
             ((and (stringp expansion) (not (equal expansion text)))
              (cons 'expand expansion))
             ((test-completion text candidates) nil)
             (t (cons 'show (all-completions text candidates))))))))))

(defun sql-datum-form-tab ()
  "Complete the field at point, or move to the next one.

`M-TAB' is the widget library's completion key, but a window manager
commonly takes it before Emacs sees it.  TAB therefore does both: it
completes while the field has something left to complete, and moves on
once it does not — so a field already holding a type, or holding a
size the suggestions do not name such as VARCHAR(120), is stepped over
rather than fought with."
  (interactive)
  (let ((todo (sql-datum--form-completion-at-point)))
    (pcase todo
      (`(expand . ,text)
       (let ((field (widget-field-at (point))))
         (delete-region (widget-field-start field) (widget-field-end field))
         (goto-char (widget-field-start field))
         (insert text)
         ;; Point sits after what was completed, ready to keep typing.
         (goto-char (+ (widget-field-start field) (length text)))))
      (`(show . ,matches)
       (with-output-to-temp-buffer "*Completions*"
         (display-completion-list matches)))
      (_ (widget-forward 1)))))

(defun sql-datum--form-has-checkbox-p (fields)
  "Return non-nil if FIELDS render any checkbox.
A box can sit in a field of its own or in the columns of a list, as
the nullable and primary-key boxes of the table builder do."
  (cl-some (lambda (f)
             (or (member (alist-get 'type f) '("bool" "multi"))
                 (cl-some (lambda (col)
                            (equal (alist-get 'type col) "bool"))
                          (alist-get 'item f))))
           fields))

(defun sql-datum-form-toggle ()
  "Toggle the checkbox at point.

SPC is where the hand already sits when working down a column of
boxes, and it is what Customize toggles with.  RET still works.
Anywhere else SPC keeps its usual meaning — which, on the form's
read-only labels, is to refuse."
  (interactive)
  (let ((widget (widget-at (point))))
    (if (and widget (eq (widget-type widget) 'checkbox))
        (widget-button-press (point))
      (call-interactively #'self-insert-command))))

(defun sql-datum--form-widget-at-point ()
  "Return the widget at point, whether it is a field or a button."
  (or (widget-field-at (point)) (widget-at (point))))

(defun sql-datum--form-line-move (dir)
  "Move DIR rows, staying in the same column where one is there to stay in.

`widget-forward' walks the widgets in the order they were created, so
from the middle of a row it steps along that row rather than down the
column — in a list of columns it slides to the last field of the row
above and stops there.  Moving by line and column instead keeps the
grid's columns, which holds because the rows are laid out to a fixed
width.  Where the adjacent line has no widget at that column — the ends
of the list, or an ordinary single-column form — it falls back to the
widget order."
  (let ((column (current-column))
        (origin (point)))
    (if (and (zerop (forward-line dir))
             (progn (move-to-column column) t)
             (/= (point) origin)
             (sql-datum--form-widget-at-point))
        (point)
      (goto-char origin)
      (widget-forward dir))))

(defun sql-datum-form-next-field ()
  "Move to the next form field."
  (interactive)
  (sql-datum--form-line-move 1))

(defun sql-datum-form-prev-field ()
  "Move to the previous form field."
  (interactive)
  (sql-datum--form-line-move -1))

(defvar sql-datum--widget-keymap nil
  "`widget-keymap' with SPC toggling a checkbox.")

(defun sql-datum--widget-keymap ()
  "Return `widget-keymap' with SPC toggling a checkbox.
The schedule and step editors predate the generic form renderer, but
their Enabled boxes should answer to the same key as its boxes do."
  (require 'wid-edit)
  (or sql-datum--widget-keymap
      (setq sql-datum--widget-keymap
            (let ((map (make-sparse-keymap)))
              (set-keymap-parent map widget-keymap)
              (define-key map (kbd "SPC") #'sql-datum-form-toggle)
              map))))

(defun sql-datum--form-make-keymap (parent)
  "Return a keymap with the wizard form bindings layered over PARENT."
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map parent)
    ;; `widget-keymap' only binds TAB/S-TAB for field motion, which leaves
    ;; the arrow keys walking character by character across protected text.
    (define-key map (kbd "<up>")    #'sql-datum-form-prev-field)
    (define-key map (kbd "<down>")  #'sql-datum-form-next-field)
    ;; A graphical Emacs sends <tab>, and only falls back to the ASCII TAB
    ;; binding when <tab> is unbound everywhere.  Configs that bind <tab>
    ;; globally (to a completion command, say) would otherwise shadow the
    ;; widget bindings throughout the form.
    ;; TAB completes where there is something to complete and moves on
    ;; otherwise; on a button there never is, so it just moves.
    (define-key map (kbd "TAB")       #'sql-datum-form-tab)
    (define-key map (kbd "<tab>")     #'sql-datum-form-tab)
    (define-key map (kbd "<backtab>") #'widget-backward)
    (define-key map (kbd "S-<tab>")   #'widget-backward)
    (define-key map (kbd "C-c C-f") #'sql-datum-form-browse)
    (define-key map (kbd "C-c C-c") #'sql-datum-form-submit)
    (define-key map (kbd "C-c C-k") #'sql-datum-form-cancel)
    map))

;; Widget fields carry their own `keymap' text property, which takes
;; precedence over the buffer's local map — so the bindings have to be
;; layered onto the field keymaps as well, not just the local one.
;;
;; These are built on first use because the maps they inherit from live
;; in wid-edit, which sql-datum loads lazily.

(defvar sql-datum--form-keymap nil
  "Keymap for datum wizard form buffers.")

(defvar sql-datum--form-field-keymap nil
  "Keymap active inside single-line wizard form fields.")

(defvar sql-datum--form-text-keymap nil
  "Keymap active inside multi-line wizard form fields.")

(defun sql-datum--form-ensure-keymaps ()
  "Build the wizard form keymaps if they do not exist yet."
  (require 'wid-edit)
  (unless sql-datum--form-keymap
    (setq sql-datum--form-keymap
          (sql-datum--form-make-keymap widget-keymap))
    ;; Only here, not on the field maps: inside a text field SPC has to
    ;; go on typing a space.  Checkboxes carry no keymap of their own,
    ;; so the buffer's map is what governs at one.
    (define-key sql-datum--form-keymap (kbd "SPC") #'sql-datum-form-toggle))
  (unless sql-datum--form-field-keymap
    (setq sql-datum--form-field-keymap
          (sql-datum--form-make-keymap widget-field-keymap)))
  (unless sql-datum--form-text-keymap
    (setq sql-datum--form-text-keymap
          (sql-datum--form-make-keymap widget-text-keymap))))

(defun sql-datum--form-protect-static-text ()
  "Make every part of the form except the editable fields read-only.
Without this the labels and help text are ordinary buffer text, so
typing outside a field silently corrupts the form layout."
  (let ((inhibit-read-only t)
        ;; Widget's after-change hook rejects edits that span fields, and
        ;; would fire on the property writes below.
        (inhibit-modification-hooks t)
        (pos (point-min)))
    (while (< pos (point-max))
      (let ((next (or (next-single-char-property-change pos 'field)
                      (point-max))))
        (unless (get-char-property pos 'field)
          (put-text-property pos next 'read-only t)
          ;; Keep the boundary characters non-sticky so that typing at the
          ;; very start or end of an adjacent field is still allowed.
          (put-text-property pos next 'front-sticky nil)
          (put-text-property pos next 'rear-nonsticky t))
        (setq pos next)))))

(defvar-local sql-datum--form-spec nil
  "The `form' alist backing the wizard form in this buffer.")

(defun sql-datum--form-field-value (spec widget)
  "Return the submitted value for field SPEC read from WIDGET."
  (let ((type (or (alist-get 'type spec) "string"))
        (raw (widget-value widget)))
    (cond
     ((equal type "bool") (if raw t :false))
     ((equal type "int")
      (if (stringp raw) (string-to-number raw) (or raw 0)))
     ;; `json-serialize' reads a plain list as an alist, so a multi-select
     ;; has to become a vector to come out as a JSON array.
     ((equal type "multi") (vconcat (and (listp raw) raw)))
     ;; A repeating group is a list of rows, and both levels have to be
     ;; vectors to serialise as nested JSON arrays.  An unticked checkbox
     ;; reads as nil, which would serialise as null rather than false.
     ((equal type "list")
      (vconcat (mapcar (lambda (row)
                         (vconcat (mapcar (lambda (v)
                                            (cond ((eq v t) t)
                                                  ((null v) :false)
                                                  (t v)))
                                          row)))
                       (and (listp raw) raw))))
     ;; A password is taken exactly as typed: trimming would silently
     ;; change a credential that legitimately has leading or trailing
     ;; whitespace.
     ((equal type "password") (or raw ""))
     ((stringp raw) (string-trim raw))
     (t raw))))

(defconst sql-datum--form-list-indent 12
  "Width of the [INS] [DEL] prefix `editable-list' puts on each row.
The legend above a column list is indented by this much so that its
headings sit over the fields they name.")

(defun sql-datum--form-list-rows (spec values)
  "Return the rows a list field will be rendered with.

Where the field tracks identity, each row gains its current name as a
trailing element, so that after editing it still says what the row was
called when the form opened."
  (let* ((key (alist-get 'key spec))
         (supplied (and values key (assoc-string key values)))
         (rows (append (and (listp (if supplied (cdr supplied)
                                     (alist-get 'default spec)))
                            (if supplied (cdr supplied)
                              (alist-get 'default spec)))
                       nil)))
    (if (alist-get 'track_identity spec)
        (mapcar (lambda (row)
                  (let ((row (append row nil)))
                    ;; Already carrying one — a form being rebuilt.
                    (if (> (length row) (length (alist-get 'item spec)))
                        row
                      (append row (list (or (car row) ""))))))
                rows)
      rows)))

(defun sql-datum--form-list-widths (item &optional rows)
  "Return the rendered width of each sub-field in ITEM.

A row only lines up if every field has a known width.  The type menu is
padded because `menu-choice' renders the chosen item's tag, so its width
would otherwise follow whatever is selected.  A text field's `:size' is
only a minimum — a longer value grows the field and pushes everything
after it out of column — so the widest value actually present is taken
into account as well."
  (let ((index -1))
    (mapcar
     (lambda (col)
       (setq index (1+ index))
       (let ((ctype (or (alist-get 'type col) "string")))
         (cond
          ;; A checkbox renders as [X], but the column has to be at
          ;; least as wide as its heading or the legend is clipped.
          ((equal ctype "bool")
           (max 3 (length (or (alist-get 'label col) ""))))
          ((equal ctype "choice")
           (apply #'max 4 (mapcar (lambda (c) (length (nth 1 c)))
                                  (alist-get 'choices col))))
          (t (apply #'max
                    (or (alist-get 'size col) 14)
                    (length (or (alist-get 'label col) ""))
                    (mapcar (lambda (row)
                              (let ((v (nth index row)))
                                (if (stringp v) (length v) 0)))
                            rows))))))
     item)))

(defun sql-datum--form-list-legend (spec &optional values)
  "Return the heading line for the column list described by SPEC."
  (let* ((item (alist-get 'item spec))
         (widths (sql-datum--form-list-widths
                  item (sql-datum--form-list-rows spec values))))
    (concat (make-string sql-datum--form-list-indent ?\s)
            (mapconcat (lambda (pair)
                         (let ((label (or (alist-get 'label (car pair)) ""))
                               (width (cdr pair)))
                           (if (> (length label) width)
                               (substring label 0 width)
                             (concat label
                                     (make-string (- width (length label))
                                                  ?\s)))))
                       (cl-mapcar #'cons item widths) " ")
            "\n")))

(defun sql-datum--form-create-widget (spec &optional values)
  "Create and return the widget for field SPEC.
A value for this field in VALUES wins over the descriptor's default, so
a form rebuilt after browsing for a path comes back as the user left it."
  (let* ((type (or (alist-get 'type spec) "string"))
         (key (alist-get 'key spec))
         ;; Field keys are strings; parsed JSON values are keyed by
         ;; symbol, and `assoc-string' matches across both.
         (supplied (and values key (assoc-string key values)))
         (default (if supplied (cdr supplied) (alist-get 'default spec)))
         (choices (alist-get 'choices spec)))
    (cond
     ((equal type "bool")
      (widget-create 'checkbox :value (and default (not (eq default :false)))))
     ((equal type "choice")
      ;; Rendered the way Customize renders a `(choice ...)' type: the
      ;; "Value Menu" button makes it visible that this is a menu, rather
      ;; than a bare value that looks like static text.
      (apply #'widget-create 'menu-choice
             :format "%[Value Menu%]: %v"
             :value (or default "")
             ;; `item' defaults to "%t\n", which leaves a blank line after
             ;; the chosen value.
             (or (mapcar (lambda (c)
                           (list 'item :format "%t"
                                 :tag (nth 1 c) :value (nth 0 c)))
                         choices)
                 '((item :format "%t" :tag "(none)" :value "")))))
     ((equal type "password")
      ;; `:secret' echoes a placeholder instead of the characters typed.
      (widget-create 'editable-field
                     :size 32
                     :secret ?*
                     :keymap sql-datum--form-field-keymap
                     :value (format "%s" (or default ""))))
     ((equal type "list")
      ;; A repeating group: [INS] and [DEL] add and remove rows, and
      ;; `widget-value' yields a list of rows.
      (widget-create
       'editable-list
       :format "%v%i\n"
       ;; JSON false parses to `:false', which is a non-nil keyword and
       ;; so would tick every checkbox it reaches.
       :value (mapcar (lambda (row)
                        (mapcar (lambda (v) (if (eq v :false) nil v)) row))
                      (sql-datum--form-list-rows spec values))
       (append
        '(group :format "%v\n")
        (cl-mapcar
         (lambda (col width)
           (let ((ctype (or (alist-get 'type col) "string")))
             (cond
              ((equal ctype "bool")
               (list 'checkbox
                     :format (concat "%[%v%]"
                                     (make-string (max 1 (- width 2)) ?\s))))
              ((equal ctype "choice")
               (append
                ;; The form's own keymap, so a global <tab> binding
                ;; cannot shadow field movement here — but the button
                ;; one, not the field one: `widget-field-keymap' binds
                ;; RET to `widget-field-activate', which on a menu does
                ;; nothing, leaving the type impossible to change.
                (list 'menu-choice :format "%[%v%] "
                      :keymap sql-datum--form-keymap)
                (mapcar (lambda (c)
                          ;; Padding the tag fixes the rendered width, so
                          ;; the fields after it stay in their columns
                          ;; whatever type is selected.
                          (let ((tag (nth 1 c)))
                            (list 'item :format "%t"
                                  :tag (concat tag
                                               (make-string
                                                (max 0 (- width (length tag)))
                                                ?\s))
                                  :value (nth 0 c))))
                        (alist-get 'choices col))))
              (t (append
                  (list 'editable-field :size width :format "%v "
                        :keymap sql-datum--form-field-keymap)
                  ;; A completing sub-field offers its candidates
                  ;; through TAB, the same as a top-level one.
                  (when (alist-get 'completions col)
                    (list :completions (alist-get 'completions col))))))))
         (alist-get 'item spec)
         (sql-datum--form-list-widths
          (alist-get 'item spec)
          (sql-datum--form-list-rows spec values)))
        ;; When the rows stand for things that already exist, each
        ;; carries what it was called when the form opened.  It renders
        ;; as nothing and navigation steps over it, but it is what lets
        ;; an edited name be told from a row deleted and another
        ;; inserted: a new row's is empty.
        (when (alist-get 'track_identity spec)
          '((string :format ""))))))
     ((equal type "multi")
      ;; A checklist: `widget-value' yields the list of ticked values.
      (apply #'widget-create 'checklist
             :format "%v"
             :value (append (and (listp default) default) nil)
             (mapcar (lambda (c)
                       (list 'item :format "%t  " :tag (nth 1 c)
                             :value (nth 0 c)))
                     choices)))
     ((equal type "completing")
      ;; An editable field with a completion table: the right shape for
      ;; lookups too long to page through as a menu.  `editable-field'
      ;; wires :completions to M-TAB through :completions-function.
      (widget-create 'editable-field
                     :size 44
                     :keymap sql-datum--form-field-keymap
                     :completions (alist-get 'completions spec)
                     :value (format "%s" (or default ""))))
     ((equal type "text")
      ;; The default format renders the tag too, and `text' defaults its
      ;; tag to the value — which prints the whole body twice.
      (widget-create 'text
                     :format "%v"
                     :size 80
                     :keymap sql-datum--form-text-keymap
                     :value (format "%s" (or default ""))))
     (t
      (widget-create 'editable-field
                     :size (if (equal type "int") 12 44)
                     :keymap sql-datum--form-field-keymap
                     :value (format "%s" (or default "")))))))

(defun sql-datum--admin-show-form (data sqli-buf)
  "Show a widget form described by DATA.
SQLI-BUF is the originating SQLi buffer.  DATA carries a `form' alist
with `fields', `values', `submit_action', and optional `notes',
`preview_action', `confirm_text' and `danger' keys."
  (require 'widget)
  (require 'wid-edit)
  (sql-datum--form-ensure-keymaps)
  (let* ((panel (alist-get 'panel data))
         (form (alist-get 'form data))
         (fields (alist-get 'fields form))
         (values (alist-get 'values form))
         (notes (alist-get 'notes form))
         (confirm-text (alist-get 'confirm_text form))
         (title (or (alist-get 'title data) "datum wizard"))
         (buf (get-buffer-create (format "*datum-admin:%s-form*" panel))))
    (with-current-buffer buf
      (kill-all-local-variables)
      (let ((inhibit-read-only t)) (erase-buffer))
      (remove-overlays)
      (widget-insert (propertize title 'face
                                 (if (alist-get 'danger form)
                                     'compilation-error 'bold)))
      (widget-insert "\n\n")
      (dolist (note notes)
        (widget-insert (propertize note 'face 'font-lock-comment-face) "\n"))
      (when notes (widget-insert "\n"))
      (let ((widgets nil)
            (label-width (apply #'max 8
                                (mapcar (lambda (f)
                                          (length (or (alist-get 'label f) "")))
                                        fields))))
        (dolist (spec fields)
          (let* ((label (or (alist-get 'label spec) ""))
                 (help (alist-get 'help spec))
                 (completions (alist-get 'completions spec))
                 (multiline (equal (alist-get 'type spec) "text"))
                 (repeating (equal (alist-get 'type spec) "list")))
            ;; Completion is invisible unless advertised.
            (when completions
              (setq help (concat (if (and help (not (string-empty-p help)))
                                     (concat help "; ") "")
                                 (format "TAB completes (%d)"
                                         (length completions)))))
            ;; A multi-line body does not belong in the label column.
            (cond
             (multiline
              (widget-insert (concat label ":"))
              (when (and help (not (string-empty-p help)))
                (widget-insert (propertize (format "  %s" help)
                                           'face 'font-lock-comment-face)))
              (widget-insert "\n"))
             ;; A repeating list needs its legend above the rows: printed
             ;; after them it lands below everything the reader is trying
             ;; to interpret, which is where it is no use.
             (repeating
              (widget-insert (concat label ":"))
              (when (and help (not (string-empty-p help)))
                (widget-insert (propertize (format "  %s" help)
                                           'face 'font-lock-comment-face)))
              (widget-insert "\n")
              (widget-insert
               (propertize (sql-datum--form-list-legend spec values)
                           'face 'font-lock-comment-face)))
             (t
              (widget-insert (format (format "%%-%ds  " label-width) label))))
            (push (cons (alist-get 'key spec)
                        (cons spec (sql-datum--form-create-widget spec values)))
                  widgets)
            ;; Paths name locations on the server, not on this machine, so
            ;; browsing has to go through the connection.
            (when (equal (alist-get 'type spec) "path")
              (widget-insert " ")
              ;; `widgets' is still being built, so the handler reads the
              ;; finished buffer-local list rather than closing over it.
              (let ((field-key (alist-get 'key spec)))
                (widget-create
                 'push-button
                 :notify (lambda (&rest _)
                           (sql-datum--admin-form-browse-path
                            panel form sql-datum--form-widgets values
                            sqli-buf field-key))
                 "Browse")))
            (when (and help (not multiline) (not repeating)
                       (not (string-empty-p (or help ""))))
              (widget-insert (propertize (format "  %s" help)
                                         'face 'font-lock-comment-face)))
            (widget-insert "\n")))
        (widget-insert "\n")
        ;; Destructive actions require retyping the object name.
        (let ((confirm-widget
               (when confirm-text
                 (widget-insert
                  (propertize (format "Type %s to confirm: " confirm-text)
                              'face 'warning))
                 (prog1 (widget-create 'editable-field
                                       :size 30
                                       :keymap sql-datum--form-field-keymap
                                       :value "")
                   (widget-insert "\n\n")))))
          (widget-create
           'push-button
           :notify (lambda (&rest _)
                     (sql-datum--admin-form-submit
                      panel form widgets values sqli-buf
                      confirm-widget confirm-text))
           (or (alist-get 'submit_label form) "Submit"))
          (when (alist-get 'preview_action form)
            (widget-insert "  ")
            (widget-create
             'push-button
             :notify (lambda (&rest _)
                       (sql-datum--admin-form-submit
                        panel form widgets values sqli-buf nil nil
                        (alist-get 'preview_action form)))
             "Preview SQL"))
          (widget-insert "  ")
          (widget-create 'push-button
                         :notify (lambda (&rest _) (quit-window t))
                         "Cancel")
          (widget-insert "\n\n")
          (widget-insert
           (propertize "up/down also move between fields\n"
                       'face 'font-lock-comment-face))
          ;; Expose the closures so the keys can reach them.
          (setq-local sql-datum--form-submit-fn
                      (lambda ()
                        (sql-datum--admin-form-submit
                         panel form widgets values sqli-buf
                         confirm-widget confirm-text)))
          (setq-local sql-datum--form-browse-fn
                      (lambda (field-key)
                        (sql-datum--admin-form-browse-path
                         panel form sql-datum--form-widgets values
                         sqli-buf field-key))))
        (setq-local sql-datum--form-widgets widgets)
        (setq-local sql-datum--form-spec form)
        (setq-local sql-datum--admin-sqli-buf sqli-buf)
        ;; A form with many fields pushes its help below the fold too.
        (setq-local header-line-format
                    (sql-datum--admin-header-line
                     nil
                     (append '(("TAB" . "complete / next field"))
                             (when (sql-datum--form-has-checkbox-p fields)
                               '(("SPC" . "toggle")))
                             (when (cl-find-if
                                    (lambda (f)
                                      (equal (alist-get 'type f) "path"))
                                    fields)
                               '(("C-c C-f" . "browse")))
                             '(("C-c C-c" . "submit")
                               ("C-c C-k" . "cancel"))))))
      (use-local-map sql-datum--form-keymap)
      ;; A wrapped row makes the layout unreadable in a narrow window,
      ;; and a column list is wider than most.
      (setq-local truncate-lines t)
      (widget-setup)
      (sql-datum--form-protect-static-text)
      ;; Start on the first field rather than on protected text.
      (goto-char (point-min))
      (widget-forward 1))
    (switch-to-buffer buf)))

(defun sql-datum--admin-form-submit (panel form widgets values sqli-buf
                                     confirm-widget confirm-text
                                     &optional override-action)
  "Collect WIDGETS and send them to PANEL's submit action.
FORM is the form spec, VALUES the server-supplied hidden values, and
SQLI-BUF the connection buffer.  When CONFIRM-TEXT is non-nil the text in
CONFIRM-WIDGET must match it.  OVERRIDE-ACTION replaces the submit action
\(used by the Preview button, which must not mutate anything)."
  (when (and confirm-text confirm-widget
             (not (equal (string-trim (widget-value confirm-widget))
                         confirm-text)))
    (user-error "Confirmation text does not match %s" confirm-text))
  (let ((payload (copy-alist (or values '()))))
    (dolist (entry widgets)
      (let ((key (car entry))
            (spec (cadr entry))
            (widget (cddr entry)))
        ;; `json-serialize' requires symbol keys; field keys arrive as
        ;; strings from `json-parse-string'.
        (push (cons (if (stringp key) (intern key) key)
                    (sql-datum--form-field-value spec widget))
              payload)))
    (let ((action (or override-action (alist-get 'submit_action form))))
      ;; The payload is base64-encoded rather than sent as raw JSON:
      ;; `_split_command_line' on the Python side does not understand
      ;; backslash escapes, so a value containing a double quote (a
      ;; Windows path, say) would otherwise be split incorrectly.
      (sql-datum--admin-send-with-payload
       sqli-buf
       (format ":admin-action %s %s" panel action)
       (base64-encode-string
        (encode-coding-string (json-serialize payload) 'utf-8) t))
      (unless override-action
        (quit-window t)
        (message "datum admin: %s sent" action)))))

;; --- Server-side path browser ---
;;
;; Data and log files live on the database server, which is usually not
;; the machine Emacs is running on, so `read-file-name' would browse the
;; wrong filesystem.  These commands ask the server to enumerate its own
;; directories instead.

(defvar-local sql-datum--browser-context nil
  "Alist describing the path browser: the directory being listed, the
form field being filled, the form values to restore, and the action that
rebuilds the form.")

(defun sql-datum--form-collect-values (widgets extra)
  "Return an alist of the current WIDGETS values, merged over EXTRA."
  (let ((payload (copy-alist (or extra '()))))
    (dolist (entry widgets)
      (let ((key (car entry)))
        (push (cons (if (stringp key) (intern key) key)
                    (sql-datum--form-field-value (cadr entry) (cddr entry)))
              payload)))
    payload))

(defun sql-datum--admin-form-browse-path (panel form widgets values
                                          sqli-buf field-key)
  "Leave the form to browse the server for a path for FIELD-KEY."
  (let* ((collected (sql-datum--form-collect-values widgets values))
         (current (cdr (assoc-string field-key collected))))
    (sql-datum--admin-send-with-payload
     sqli-buf
     (format ":admin-action %s browse-path" panel)
     (base64-encode-string
      (encode-coding-string
       (json-serialize
        `((path . ,(if (stringp current) current ""))
          (field . ,field-key)
          (values . ,collected)
          (return_action . ,(or (alist-get 'submit_action form)
                                "new-database"))))
       'utf-8)
      t))))

(defvar sql-datum--path-browser-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "RET") #'sql-datum-path-browser-open)
    (define-key map "f"         #'sql-datum-path-browser-open)
    (define-key map "^"         #'sql-datum-path-browser-up)
    (define-key map "u"         #'sql-datum-path-browser-up)
    (define-key map "s"         #'sql-datum-path-browser-select)
    (define-key map "n"         #'next-line)
    (define-key map "p"         #'previous-line)
    (define-key map (kbd "<down>") #'next-line)
    (define-key map (kbd "<up>")   #'previous-line)
    ;; Bound for the same reason as in the form: a global <tab> binding
    ;; would otherwise shadow movement here too.
    (define-key map (kbd "<tab>")     #'next-line)
    (define-key map (kbd "<backtab>") #'previous-line)
    (define-key map "q"         #'sql-datum-path-browser-cancel)
    map)
  "Keymap for the datum server path browser.")

(define-derived-mode sql-datum--path-browser-mode special-mode "datum-paths"
  "Major mode for browsing directories on the database server."
  (setq truncate-lines t))

(defun sql-datum--path-browser-send (path &optional field)
  "Re-list PATH on the server, keeping the pending form values."
  (let ((ctx sql-datum--browser-context))
    (sql-datum--admin-send-with-payload
     sql-datum--admin-sqli-buf
     ":admin-action databases browse-path"
     (base64-encode-string
      (encode-coding-string
       (json-serialize
        `((path . ,(or path ""))
          (field . ,(or field (alist-get 'field ctx) ""))
          (values . ,(or (alist-get 'values ctx) '()))
          (return_action . ,(or (alist-get 'return_action ctx)
                                "new-database"))))
       'utf-8)
      t))))

(defun sql-datum--path-browser-entry ()
  "Return (PATH . IS-DIR) for the line at point, or nil."
  (let ((path (get-text-property (line-beginning-position) 'sql-datum-row-id))
        (dir (get-text-property (line-beginning-position) 'sql-datum-is-dir)))
    (when path (cons path dir))))

(defun sql-datum-path-browser-open ()
  "Descend into the directory at point."
  (interactive)
  (let ((entry (sql-datum--path-browser-entry)))
    (unless entry (user-error "No entry at point"))
    (unless (cdr entry) (user-error "Not a directory: %s" (car entry)))
    (sql-datum--path-browser-send (car entry))))

(defun sql-datum-path-browser-up ()
  "Go to the parent directory."
  (interactive)
  (goto-char (point-min))
  (if (search-forward-regexp "^.*\\.\\.$" nil t)
      (sql-datum-path-browser-open)
    (user-error "Already at the top")))

(defun sql-datum--path-browser-return (chosen)
  "Rebuild the form, setting the browsed field to CHOSEN when non-nil."
  (let* ((ctx sql-datum--browser-context)
         (field (alist-get 'field ctx))
         (values (copy-alist (or (alist-get 'values ctx) '())))
         (action (or (alist-get 'return_action ctx) "new-database")))
    (when (and chosen field)
      ;; Pushed to the head so it wins over the stale value behind it.
      (push (cons (intern field) chosen) values))
    (sql-datum--admin-send-with-payload
     sql-datum--admin-sqli-buf
     (format ":admin-action databases %s" action)
     (base64-encode-string
      (encode-coding-string
       (json-serialize `((values . ,values))) 'utf-8)
      t))
    (quit-window t)))

(defun sql-datum-path-browser-select ()
  "Use the directory being listed, and return to the form."
  (interactive)
  (let* ((entry (sql-datum--path-browser-entry))
         (parent-row (get-text-property (line-beginning-position)
                                        'sql-datum-is-parent))
         ;; A subdirectory row selects that directory without having to
         ;; open it first; ".." and file rows take the listed directory.
         (chosen (if (and entry (cdr entry) (not parent-row))
                     (car entry)
                   (alist-get 'path sql-datum--browser-context))))
    (sql-datum--path-browser-return chosen)))

(defun sql-datum-path-browser-cancel ()
  "Return to the form without changing the path."
  (interactive)
  (sql-datum--path-browser-return nil))

(defvar sql-datum--fs-cache (make-hash-table :test 'equal)
  "Files already pulled off a server, keyed by connection and path.

Each value is a plist of :local, :size and :modified.  A listing row
already carries the size and the time the server last reported, so
deciding whether a copy is still good costs nothing — no extra round
trip to ask.")

(defun sql-datum--fs-download-root ()
  "Return the directory downloaded files are kept in, creating it."
  (let ((root (or sql-datum-download-directory
                  (expand-file-name "datum-files"
                                    temporary-file-directory))))
    (unless (file-directory-p root)
      (make-directory root t))
    root))

(defun sql-datum--fs-cache-key (path &optional sqli-buf)
  "Return the cache key for PATH on a connection.
SQLI-BUF defaults to the one this buffer was filled from."
  (let ((buf (or sqli-buf sql-datum--admin-sqli-buf)))
    (concat (if (buffer-live-p buf) (buffer-name buf) "?") "|" path)))

(defun sql-datum--fs-local-name (path &optional sqli-buf)
  "Return a local path to keep PATH at.

The basename is kept so that Emacs still recognises the file by its
extension — a zip has to look like a zip for `archive-mode\=' to open
it — and the directories above it are replaced by a hash of the
server path, so two files of the same name do not collide."
  (let* ((trimmed (replace-regexp-in-string "[/\\\\]+\\'" "" path))
         (base (if (string-match "\\([^/\\\\]+\\)\\'" trimmed)
                   (match-string 1 trimmed)
                 "file"))
         (key (sql-datum--fs-cache-key path sqli-buf)))
    (expand-file-name (concat (substring (md5 key) 0 10) "-" base)
                      (sql-datum--fs-download-root))))

(defun sql-datum--fs-cached (path size modified)
  "Return the local copy of PATH when it is still good, else nil.

SIZE and MODIFIED are what the listing last reported."
  (let ((entry (gethash (sql-datum--fs-cache-key path) sql-datum--fs-cache)))
    (when (and entry
               (file-readable-p (plist-get entry :local))
               ;; Both have to match: a file rewritten to the same length
               ;; still moves its timestamp.
               (equal (plist-get entry :size) (format "%s" size))
               (equal (plist-get entry :modified) (format "%s" modified)))
      (plist-get entry :local))))

(defun sql-datum--fs-cache-put (path local size modified &optional sqli-buf)
  "Remember that PATH is held at LOCAL, as of SIZE and MODIFIED."
  (puthash (sql-datum--fs-cache-key path sqli-buf)
           (list :local local
                 :size (format "%s" size)
                 :modified (format "%s" modified))
           sql-datum--fs-cache))

(defvar sql-datum--admin-file-path nil
  "Path of the server file shown in this buffer.")
(make-variable-buffer-local 'sql-datum--admin-file-path)

(defvar sql-datum-file-view-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "w" #'sql-datum-file-view-copy-path)
    (define-key map "B" #'sql-datum-admin-back)
    map)
  "Keymap for a buffer showing a server file.")

(define-derived-mode sql-datum-file-view-mode special-mode "datum-file"
  "Major mode for viewing a file read from the server."
  (setq buffer-read-only t))

(defun sql-datum-file-view-copy-path ()
  "Copy the path of the file being viewed to the kill ring."
  (interactive)
  (unless sql-datum--admin-file-path (user-error "No file here"))
  (kill-new sql-datum--admin-file-path)
  (message "datum: %s" sql-datum--admin-file-path))

(defun sql-datum--admin-show-file (data sqli-buf)
  "Show the contents of a server file described by DATA.
SQLI-BUF is the originating SQLi buffer."
  (let* ((path (or (alist-get 'path (alist-get 'context data)) ""))
         (content (or (alist-get 'content data) ""))
         (buf (get-buffer-create
               (format "*datum-file: %s*" (file-name-nondirectory
                                           (directory-file-name path))))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert content)
        (goto-char (point-min)))
      (sql-datum-file-view-mode)
      (setq sql-datum--admin-file-path path)
      (setq sql-datum--admin-sqli-buf sqli-buf)
      ;; The panel this came from, so B goes back to the listing.
      (setq sql-datum--admin-panel-data data)
      ;; The helper takes the panel's action list, not a string, so the
      ;; description is prepended rather than passed in.
      (setq-local header-line-format
                  (concat " " (or (alist-get 'info data) path) "  "
                          (sql-datum--admin-header-line
                           nil
                           '(("w" . "copy path") ("B" . "back")
                             ("q" . "quit"))))))
    (switch-to-buffer buf)))

(defun sql-datum--admin-show-path-browser (data sqli-buf)
  "Display a server directory listing from DATA."
  (let* ((rows (alist-get 'rows data))
         (title (or (alist-get 'title data) "Browse"))
         (info (alist-get 'info data))
         (context (alist-get 'context data))
         (buf (get-buffer-create "*datum-admin:path-browser*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (sql-datum--path-browser-mode)
        (insert (propertize title 'face 'bold) "\n")
        (when info
          (insert (propertize info 'face 'font-lock-comment-face) "\n"))
        (insert "\n")
        (dolist (row rows)
          (let* ((is-dir (not (string-empty-p (or (nth 0 row) ""))))
                 (name (nth 1 row))
                 (path (nth 2 row))
                 (start (point)))
            (insert (if is-dir "  [dir]  " "         ")
                    (propertize name 'face
                                (if is-dir 'font-lock-function-name-face
                                  'default)))
            (put-text-property start (point) 'sql-datum-row-id path)
            (put-text-property start (point) 'sql-datum-is-dir is-dir)
            ;; Flagged by name: the parent's path gives no hint that it is
            ;; the ".." row rather than an ordinary directory.
            (when (equal name "..")
              (put-text-property start (point) 'sql-datum-is-parent t))
            (insert "\n")))
        (insert "\n"
                (propertize "n/p or arrows move between entries\n"
                            'face 'font-lock-comment-face)))
      (setq sql-datum--browser-context context
            sql-datum--admin-sqli-buf sqli-buf)
      (setq-local header-line-format
                  (sql-datum--admin-header-line
                   nil
                   '(("RET" . "open") ("^" . "up")
                     ("s" . "select this directory") ("q" . "cancel"))))
      (goto-char (point-min))
      (forward-line (if info 3 2)))
    (pop-to-buffer buf)))

;; --- Auto-refresh ---

(defun sql-datum--admin-send-refresh ()
  "Send the refresh command for the current admin panel."
  (let ((panel sql-datum--admin-panel-name)
        (context sql-datum--admin-context))
    (when panel
      (let ((cmd (if context
                     ;; For sub-panels, reconstruct the full command
                     (let ((sub (alist-get 'sub_panel sql-datum--admin-panel-data)))
                       (cond
                        ((and (equal sub "detail")
                              (alist-get 'job_name context))
                         (format ":admin jobs detail %s"
                                 (alist-get 'job_name context)))
                        ((and (equal sub "history")
                              (alist-get 'job_name context))
                         (format ":admin jobs history %s"
                                 (alist-get 'job_name context)))
                        ((and (equal sub "executions")
                              (alist-get 'package_name context))
                         (format ":admin ssis executions %s"
                                 (alist-get 'package_name context)))
                        ((and (equal sub "files")
                              (alist-get 'database context))
                         (format ":admin-action databases files %s"
                                 (alist-get 'database context)))
                        ((and (equal sub "user-mappings")
                              (alist-get 'login context))
                         (format ":admin-action security mappings %s"
                                 (alist-get 'login context)))
                        ;; Refreshing must stay on the principal and
                        ;; database being looked at, not fall back to
                        ;; the login list.
                        ((and (equal sub "permissions")
                              (alist-get 'principal context))
                         (format ":admin-action security permissions %s"
                                 (sql-datum--admin-payload
                                  (let ((db (alist-get 'database context)))
                                    (append
                                     `((principal . ,(alist-get 'principal
                                                                context)))
                                     (when (and db (not (string-empty-p db)))
                                       `((database . ,db))))))))
                        ;; A directory listing has to refresh at the
                        ;; directory being shown, or every tick would
                        ;; walk back to the default one.
                        ((and (equal panel "filesystem")
                              (alist-get 'path context))
                         (format ":admin filesystem %s"
                                 (alist-get 'path context)))
                        (t (format ":admin %s" panel))))
                   (format ":admin %s" panel))))
        (sql-datum--admin-send-command cmd)))))

(defun sql-datum--admin-send-command (cmd)
  "Send CMD to the datum process associated with this admin buffer."
  (sql-datum--admin-send-command-to sql-datum--admin-sqli-buf cmd))

(defun sql-datum--admin-send-command-to (sqli-buf cmd)
  "Send CMD to the datum process in SQLI-BUF."
  (let ((buf (or (and sqli-buf
                      (buffer-live-p sqli-buf)
                      sqli-buf)
                 (let ((b (sql-find-sqli-buffer 'datum)))
                   (and b (get-buffer b))))))
    (if (and buf (get-buffer-process buf))
        (with-current-buffer buf
          (sql-datum--enqueue-one cmd :silent t :priority :low))
      (message "datum admin: no active connection for command"))))

(defconst sql-datum--max-command-length 3900
  "Longest command line sent to the datum process.

A pty in canonical mode truncates input at 4095 bytes and says nothing
about it, so a longer command arrives cut in half — a base64 payload
then fails to decode.  Anything above this is sent in chunks instead,
with headroom left for the command around them.")

(defun sql-datum--admin-send-commands-to (sqli-buf commands)
  "Send COMMANDS to the datum process in SQLI-BUF as one transaction.
Queuing them together keeps them in order and stops anything else being
interleaved between them."
  (let ((buf (or (and sqli-buf (buffer-live-p sqli-buf) sqli-buf)
                 (let ((b (sql-find-sqli-buffer 'datum)))
                   (and b (get-buffer b))))))
    (if (and buf (get-buffer-process buf))
        (with-current-buffer buf
          (sql-datum--enqueue (list :commands commands
                                    :silent t
                                    :priority :low)))
      (message "datum admin: no active connection for command"))))

(defun sql-datum--admin-send-with-payload (sqli-buf prefix payload)
  "Send PREFIX with PAYLOAD appended, chunking PAYLOAD when it is long.

A payload that would push the line past what a pty carries is sent as
`:admin-payload' chunks first, and PREFIX then refers to them with
@payload."
  (if (<= (+ (length prefix) (length payload) 1)
          sql-datum--max-command-length)
      (sql-datum--admin-send-command-to
       sqli-buf (concat prefix " " payload))
    (let ((size (- sql-datum--max-command-length 20))
          (start 0)
          (chunks nil))
      (while (< start (length payload))
        (let ((end (min (length payload) (+ start size))))
          (push (concat ":admin-payload " (substring payload start end))
                chunks)
          (setq start end)))
      (sql-datum--admin-send-commands-to
       sqli-buf (append (nreverse chunks)
                        (list (concat prefix " @payload")))))))

(defun sql-datum--admin-start-timer (buf)
  "Start the auto-refresh timer for admin buffer BUF."
  (sql-datum--admin-stop-timer buf)
  (when sql-datum-admin-refresh-interval
    (with-current-buffer buf
      (setq sql-datum--admin-timer
            (run-with-timer sql-datum-admin-refresh-interval
                            sql-datum-admin-refresh-interval
                            #'sql-datum--admin-tick buf)))))

(defun sql-datum--admin-stop-timer (buf)
  "Stop the auto-refresh timer for admin buffer BUF."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (when sql-datum--admin-timer
        (cancel-timer sql-datum--admin-timer)
        (setq sql-datum--admin-timer nil)))))

(defun sql-datum--admin-tick (buf)
  "Timer callback: refresh admin buffer BUF if it still exists.
Skip refresh while the minibuffer is active (e.g. company-mode,
completing-read) to avoid cursor position disruption."
  (if (buffer-live-p buf)
      (unless (active-minibuffer-window)
        (with-current-buffer buf
          (sql-datum--admin-send-refresh)))
    ;; Buffer killed — stop timer
    (when (timerp sql-datum--admin-timer)
      (cancel-timer sql-datum--admin-timer))))

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

(defun sql-datum--last-unquoted-dot (name)
  "Return the position of the last dot in NAME that is outside quotes.
Returns nil if there is no unquoted dot."
  (let ((i (1- (length name)))
        (in-quote nil)
        (result nil))
    ;; Scan backwards looking for an unquoted dot.
    (while (and (>= i 0) (not result))
      (let ((ch (aref name i)))
        (cond
         (in-quote
          (when (or (eq ch ?\") (eq ch ?\`) (eq ch ?\[))
            (setq in-quote nil)))
         ((or (eq ch ?\") (eq ch ?\`) (eq ch ?\]))
          (setq in-quote t))
         ((eq ch ?.)
          (setq result i))))
      (setq i (1- i)))
    result))

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
  ;; Fast path: plain string-prefix-p handles the common unquoted case.
  (or (string-prefix-p prefix candidate t)
      ;; After-dot match: "Pat" matches "dbo.PatientDim" via the
      ;; portion after the last dot.  Use `string-search' (cheap) to
      ;; find the last dot instead of full split-identifier parsing.
      (and (not (string-match-p "\\." prefix))
           (let ((dot-pos (sql-datum--last-unquoted-dot candidate)))
             (and dot-pos
                  ;; Skip default-schema candidates — bare forms are
                  ;; already separate candidates in the list.
                  (not (and ds-prefix (string-prefix-p ds-prefix candidate t)))
                  (string-prefix-p prefix (substring candidate (1+ dot-pos)) t))))
      ;; Slow path: only when quotes are present in prefix or candidate.
      (let ((has-quotes (or (string-match-p "[\"\\[`]" prefix)
                            (string-match-p "[\"\\[`]" candidate))))
        (when has-quotes
          (let ((bare-prefix (sql-datum--unquote-identifier prefix))
                (bare-candidate (sql-datum--unquote-identifier candidate)))
            (or (string-prefix-p bare-prefix bare-candidate t)
                (and (not (string-match-p "\\." prefix))
                     (string-match-p "\\." candidate)
                     (not (and ds-prefix (string-prefix-p ds-prefix candidate t)))
                     (let ((after-dot (car (last (sql-datum--split-identifier
                                                  candidate)))))
                       (string-prefix-p
                        bare-prefix
                        (sql-datum--unquote-part after-dot)
                        t)))))))))

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
       (let ((matches (let (result)
                        (dolist (c candidates)
                          (when (and (sql-datum--completion-match-p string c ds-prefix)
                                     (or (null pred) (funcall pred c)))
                            (push c result)))
                        (nreverse result))))
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
Skips if TABLE is already cached, already in-flight, or already
attempted (via `sql-datum--columns-fetched').

Used for background prefetching (e.g. inside dynamic completion
tables).  For completion-time fetches, prefer the synchronous
`sql-datum--fetch-columns-sync' which blocks briefly with timeout."
  ;; All cache keys are downcased — normalize the lookup key.
  (let ((key (downcase table))
        (fetched-hash (buffer-local-value 'sql-datum--columns-fetched buf)))
    (sql-datum--trace "fetch-columns-async: table=%s key=%s cached=%s pending=%s ready=%s"
                      table key
                      (if (gethash key col-hash) "yes" "no")
                      (gethash key pending-hash)
                      (buffer-local-value 'sql-datum--ready buf))
    (unless (or (gethash key col-hash)
                (gethash key pending-hash)
                (gethash key fetched-hash))
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
  "Fetch columns for TABLES, waiting until all arrive or timeout.
Sends :columns TABLE :silent for each uncached table via the command
queue, then polls with `accept-process-output' until all columns are
cached or `sql-datum-column-fetch-timeout' expires.
Tables already in `sql-datum--columns-fetched' are skipped to prevent
repeated fetch attempts across capf invocations."
  (let* ((proc (get-buffer-process buf))
         (col-hash (buffer-local-value 'sql-datum--columns buf))
         (pending-hash (buffer-local-value 'sql-datum--columns-pending buf))
         (fetched-hash (buffer-local-value 'sql-datum--columns-fetched buf)))
    (when (and proc col-hash pending-hash)
      (let ((needed nil))
        ;; Enqueue fetches for uncached tables via the normal queue path.
        ;; Skip tables we have already attempted (even if they timed out).
        (dolist (tbl tables)
          (let ((key (downcase tbl)))
            (unless (or (gethash key col-hash)
                        (gethash key fetched-hash))
              (push key needed)
              (puthash key t fetched-hash)
              (unless (gethash key pending-hash)
                (sql-datum--fetch-columns-send tbl key buf proc pending-hash)))))
        (if (null needed)
            t
          ;; Pump the queue and wait.
          (with-current-buffer buf (sql-datum--queue-pump))
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
            (dolist (tbl tables)
              (remhash (downcase tbl) pending-hash))
            (if needed
                (progn
                  (sql-datum--trace "fetch-columns-sync: TIMEOUT waiting for %s" needed)
                  ;; Suppress warning when queue is still busy — data will
                  ;; arrive once the current transaction completes.
                  (when (and (null (buffer-local-value 'sql-datum--queue-current buf))
                             (null (buffer-local-value 'sql-datum--command-queue buf)))
                    (message "datum: timed out waiting for column data for: %s"
                             (mapconcat #'identity needed ", ")))
                  nil)
              (sql-datum--trace "fetch-columns-sync: all columns cached")
              t)))))))

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

(defun sql-datum--xdb-completion (prefix start end buf dbs xdb-cache dialect)
  "Handle database-qualified (three-part) name completion.
PREFIX is the typed text, START/END delimit it.  BUF is the SQLi
buffer.  DBS is the database list, XDB-CACHE the cross-db hash.
DIALECT is the current SQL dialect string.
For the current database, candidates are synthesized from the main
cache (works for all dialects).  For a different database, only
MSSQL is supported (async cross-db fetch).
Returns a completion spec or nil if PREFIX is not a known database."
  (let* ((first-dot (string-match "\\." prefix))
         (db-part (and first-dot (substring prefix 0 first-dot)))
         ;; Use canonical (server-side) casing for the database name
         (db-match (and db-part
                        (car (sql-datum--member-ignore-case
                              db-part dbs))))
         (current-db (and db-match buf
                          (gethash "database"
                                   (buffer-local-value 'sql-datum--meta buf)
                                   ""))))
    (when db-match
      (if (string-equal-ignore-case db-match current-db)
          ;; Same database: synthesize three-part candidates from main cache
          (let* ((tables   (buffer-local-value 'sql-datum--tables buf))
                 (routines (buffer-local-value 'sql-datum--routines buf))
                 (db-pfx   (concat db-match "."))
                 (all-cands (mapcar (lambda (c) (concat db-pfx c))
                                    (append tables routines)))
                 (cur-rtypes (buffer-local-value 'sql-datum--routine-types buf))
                 (cur-sigs   (buffer-local-value 'sql-datum--routine-signatures buf))
                 ;; Capture for closures
                 (the-dialect dialect)
                 (the-tables  (mapcar (lambda (c) (concat db-pfx c)) tables))
                 (the-routines (mapcar (lambda (c) (concat db-pfx c)) routines)))
            (list start end
                  (sql-datum--make-completion-table all-cands)
                  :exclusive t
                  :annotation-function
                  (lambda (cand)
                    (cond ((member cand the-tables)   " [table]")
                          ((member cand the-routines) " [routine]")
                          (t "")))
                  :exit-function
                  (lambda (cand status)
                    (when (eq status 'finished)
                      (sql-datum--maybe-quote-completed cand start the-dialect)
                      (let ((dc (downcase cand)))
                        (when (and cur-rtypes
                                   (equal (gethash dc cur-rtypes) "FUNCTION"))
                          (insert "()")
                          (backward-char)
                          (when (and cur-sigs (gethash dc cur-sigs))
                            (message "%s(%s)" cand
                                     (gethash dc cur-sigs)))))))))
        ;; Different database: only MSSQL supports cross-db fetch
        (when (equal dialect "mssql")
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
                                       (gethash dc xsigs)))))))))))))))


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
               ;; Check for database-qualified prefix (any dialect).
               ;; Use raw-prefix so trailing dots are preserved
               ;; (e.g. "CDW_NEW." keeps the dot for db-part extraction).
               (xdb-result (when (and (string-match-p "\\." raw-prefix)
                                      dbs)
                             (sql-datum--xdb-completion
                              raw-prefix start end buf dbs xdb-cache dialect))))
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
                  ;; Clear the fetched guard for the target table so that
                  ;; qualified column completion always gets a fresh attempt.
                  (let ((fetched-hash (and buf (buffer-local-value
                                                'sql-datum--columns-fetched buf)))
                        (all-tables (cons resolved
                                         (cl-remove resolved
                                                    (mapcar #'cdr aliases)
                                                    :test #'string-equal-ignore-case))))
                    (when fetched-hash
                      (remhash resolved-key fetched-hash))
                    (if (sql-datum--explicit-completion-p)
                        (sql-datum--fetch-columns-sync all-tables buf)
                      (dolist (tbl all-tables)
                        (sql-datum--fetch-columns-async
                         tbl buf col-hash
                         (buffer-local-value
                          'sql-datum--columns-pending buf)))))
                  ;; Only return column completion when columns are
                  ;; actually cached; otherwise fall through to normal
                  ;; completion so the user still gets useful candidates.
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
                  ;; Cache state for the dynamic completion table.
                  ;; These variables track the last-seen source lists
                  ;; (by identity) so we only rebuild candidates and
                  ;; hash sets when introspection data actually changes.
                  (let ((cache--tables nil)
                        (cache--schemas nil)
                        (cache--routines nil)
                        (cache--col-hash nil)
                        (cache--col-count nil)
                        (cache--dbs nil)
                        (cache--comp-table nil))
                  (list start end
                        (lambda (string pred action)
                          ;; Check if source data has changed (identity check).
                          ;; Lists are replaced wholesale on refresh, so `eq'
                          ;; detects staleness cheaply.
                          (let* ((cur-tables (and buf (buffer-local-value 'sql-datum--tables buf)))
                                 (cur-schemas (and buf (buffer-local-value 'sql-datum--schemas buf)))
                                 (cur-routines (and buf (buffer-local-value 'sql-datum--routines buf)))
                                 (cur-col-hash (and buf (buffer-local-value 'sql-datum--columns buf)))
                                 (cur-col-count (when cur-col-hash (hash-table-count cur-col-hash)))
                                 (cur-dbs (and buf (buffer-local-value 'sql-datum--databases buf)))
                                 (stale (or (not (eq cur-tables cache--tables))
                                            (not (eq cur-schemas cache--schemas))
                                            (not (eq cur-routines cache--routines))
                                            (not (eq cur-col-hash cache--col-hash))
                                            (not (eql cur-col-count cache--col-count))
                                            (not (eq cur-dbs cache--dbs)))))
                            (when stale
                              ;; Update identity trackers
                              (setq cache--tables cur-tables
                                    cache--schemas cur-schemas
                                    cache--routines cur-routines
                                    cache--col-hash cur-col-hash
                                    cache--col-count cur-col-count
                                    cache--dbs cur-dbs)
                              ;; Rebuild derived data
                              (let* ((cur-columns (when cur-col-hash
                                                    (let (all)
                                                      (maphash (lambda (_k v)
                                                                 (setq all (append v all)))
                                                               cur-col-hash)
                                                      (delete-dups all))))
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
                                     ;; Re-check for async-fetched columns
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
                                                             cur-dbs)
                                                   (append cur-tables bare-tables cur-schemas
                                                           cur-routines bare-routines
                                                           effective-columns
                                                           cur-dbs)))
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
                                ;; Cache the built completion table
                                (setq cache--comp-table (sql-datum--make-completion-table
                                                         candidates sort-fn ds-prefix))))
                            ;; Delegate to cached completion table
                            (when cache--comp-table
                              (funcall cache--comp-table string pred action))))
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
                                  (message "%s" msg)))))))))))))))))


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
  (let ((sql-login-delay 0)
        ;; Capture and reset per-connection env so it doesn't leak
        ;; to subsequent connections that don't set it.
        (conn-environment sql-datum-environment)
        (use-bcp sql-datum-bcp)
        (bcp-extra sql-datum-bcp-extra))
    (setq sql-datum-environment nil)
    (setq sql-datum-bcp nil)
    (setq sql-datum-bcp-extra nil)
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
      ;; Enable bcp for bulk CSV export when requested.
      (when use-bcp
        (setf parameters (append parameters (list "--bcp"))))
      ;; Append --bcp-extra flags for per-connection bcp settings.
      (when bcp-extra
        (setf parameters
              (append parameters
                      (mapcan (lambda (arg) (list "--bcp-extra" arg))
                              bcp-extra))))
      ;; Inject per-connection environment variables (e.g. KRB5CCNAME)
      ;; by let-binding process-environment around the subprocess launch.
      (let ((process-environment
             (if conn-environment
                 (append (mapcar (lambda (pair)
                                   (format "%s=%s" (car pair) (cdr pair)))
                                 conn-environment)
                         process-environment)
               process-environment)))
        (sql-comint product parameters buf-name))
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
            ;; Compare unquoted forms so that an identifier like
            ;; "test table" matches the candidate public."test table".
            (cl-flet ((unquoted-equal
                        (a b)
                        (string-equal-ignore-case
                         a (sql-datum--unquote-identifier b))))
              (or (cl-find ident all-tables :test #'unquoted-equal)
                  ;; Bare name may need the default schema prefix to match.
                  (let ((ds (and buf-obj (buffer-local-value
                                          'sql-datum--default-schema buf-obj))))
                    (when ds
                      (cl-find (concat ds "." ident) all-tables
                               :test #'unquoted-equal)))
                  ;; 3-part name (db.schema.table) — try schema.table in current db.
                  (when (>= (length parts) 3)
                    (let ((schema-table (mapconcat #'identity
                                                   (last parts 2) ".")))
                      (cl-find schema-table all-tables
                               :test #'unquoted-equal))))))))
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

;;; ---------------------------------------------------------------------------
;;; Table wizards from a SQL buffer
;;; ---------------------------------------------------------------------------

;; The table builder and editor live in the schema admin panel, reached
;; by walking C-c s a -> schema -> tables.  These open them straight
;; from the query buffer on the table already under the cursor, the way
;; the rest of the C-c t tools work.

(defun sql-datum--admin-payload (alist)
  "Encode ALIST as the base64 JSON payload an admin action takes."
  (base64-encode-string
   (encode-coding-string (json-serialize alist) 'utf-8) t))

(defun sql-datum--read-schema (prompt)
  "Read a schema name with completion from the introspection cache.
The schema of the identifier at point is offered as the default when it
names a known schema."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (buf-obj (and buf (get-buffer buf)))
         (schemas (and buf-obj (buffer-local-value 'sql-datum--schemas
                                                   buf-obj)))
         (ident (sql-datum--identifier-at-point))
         (parts (and ident (sql-datum--split-identifier ident)))
         (default (when (cdr parts)
                    (let ((head (sql-datum--unquote-part (car parts))))
                      (car (member head schemas))))))
    (completing-read prompt schemas nil nil nil nil default)))

(defun sql-datum--table-parts (table)
  "Split TABLE into a (SCHEMA . NAME) pair for an admin action.

A bare name is resolved against the introspection cache, since the
wizards address a table by schema and name rather than by the string
the query buffer happens to use.  A three-part name names another
database, which these wizards do not reach."
  (let* ((parts (mapcar #'sql-datum--unquote-part
                        (sql-datum--split-identifier table))))
    (pcase (length parts)
      (3 (user-error
          "%s is in another database — switch with C-c u first" table))
      (2 (cons (nth 0 parts) (nth 1 parts)))
      (1 (let* ((buf (sql-find-sqli-buffer 'datum))
                (buf-obj (and buf (get-buffer buf)))
                (cached (and buf-obj
                             (buffer-local-value 'sql-datum--tables buf-obj)))
                (name (car parts))
                ;; Every cached "schema.name" whose name half matches.
                (matches
                 (cl-remove-if-not
                  (lambda (cand)
                    (let ((cp (mapcar #'sql-datum--unquote-part
                                      (sql-datum--split-identifier cand))))
                      (and (= 2 (length cp))
                           (cl-equalp name (nth 1 cp)))))
                  cached)))
           (pcase (length matches)
             (0 (user-error
                 "Don't know which schema %s is in — qualify it" name))
             (1 (let ((cp (mapcar #'sql-datum--unquote-part
                                  (sql-datum--split-identifier
                                   (car matches)))))
                  (cons (nth 0 cp) (nth 1 cp))))
             ;; The same name in several schemas: let the user say.
             (_ (let ((pick (completing-read
                             (format "Which %s? " name) matches nil t)))
                  (let ((cp (mapcar #'sql-datum--unquote-part
                                    (sql-datum--split-identifier pick))))
                    (cons (nth 0 cp) (nth 1 cp))))))))
      (_ (user-error "Cannot make sense of table name: %s" table)))))

(defun sql-datum-new-table (schema)
  "Open the table builder for a new table in SCHEMA."
  (interactive (list (sql-datum--read-schema "New table in schema: ")))
  (when (string-empty-p (string-trim schema))
    (user-error "No schema given"))
  (sql-datum--admin-send-command-to
   nil (format ":admin-action schema new-table %s" schema)))

(defun sql-datum-edit-table (table)
  "Open the table editor on TABLE, altering the columns it already has."
  (interactive (list (sql-datum--read-table "Edit table: ")))
  (let ((parts (sql-datum--table-parts table)))
    (sql-datum--admin-send-command-to
     nil (format ":admin-action schema edit-table %s"
                 (sql-datum--admin-payload
                  `((schema . ,(car parts)) (table . ,(cdr parts))))))))

;;; ---------------------------------------------------------------------------
;;; Database, schema and security wizards from a SQL buffer
;;; ---------------------------------------------------------------------------

(defun sql-datum--read-database (prompt)
  "Read a database name with completion from the introspection cache.
PROMPT is shown without a default; the database named by the first
segment of the identifier at point is offered as one when it is known."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (buf-obj (and buf (get-buffer buf)))
         (databases (and buf-obj (buffer-local-value 'sql-datum--databases
                                                     buf-obj)))
         (ident (sql-datum--identifier-at-point))
         (first-seg (when ident (car (split-string ident "\\." t))))
         (default (when (and first-seg databases)
                    (cl-find first-seg databases
                             :test #'string-equal-ignore-case))))
    (completing-read (if default
                         (format "%s(default %s) " prompt default)
                       prompt)
                     databases nil nil nil nil default)))

(defun sql-datum--read-principal (prompt)
  "Read a login or role name, completing against what the panel has seen.
The list fills in from the security panel, so until it has been opened
on this connection the name is free text."
  (let* ((buf (sql-find-sqli-buffer 'datum))
         (buf-obj (and buf (get-buffer buf)))
         (principals (and buf-obj
                          (buffer-local-value 'sql-datum--principals
                                              buf-obj))))
    (completing-read prompt principals)))

(defun sql-datum--admin-request (panel cmd)
  "Send CMD and let PANEL show itself when its data comes back."
  (setq sql-datum--admin-display-request panel)
  (sql-datum--admin-send-command-to nil cmd))

;; --- Databases ---

(defun sql-datum-new-database ()
  "Open the wizard for creating a database."
  (interactive)
  (sql-datum--admin-send-command-to
   nil ":admin-action databases new-database"))

(defun sql-datum-alter-database (database)
  "Open the wizard for altering DATABASE."
  (interactive (list (sql-datum--read-database "Alter database: ")))
  (sql-datum--admin-send-command-to
   nil (format ":admin-action databases edit-database %s" database)))

(defun sql-datum-drop-database (database)
  "Drop DATABASE, by way of the panel that says what that would take.
Nothing is dropped until that panel is confirmed."
  (interactive (list (sql-datum--read-database "Drop database: ")))
  (sql-datum--admin-request
   "databases"
   (format ":admin-action databases drop-check %s" database)))

(defun sql-datum-backup-database (database)
  "Open the backup wizard for DATABASE."
  (interactive (list (sql-datum--read-database "Back up database: ")))
  (sql-datum--admin-send-command-to
   nil (format ":admin-action databases new-backup %s"
               (sql-datum--admin-payload `((database . ,database))))))

(defun sql-datum-restore-database (database)
  "Open the restore wizard for DATABASE."
  (interactive (list (sql-datum--read-database "Restore database: ")))
  (sql-datum--admin-send-command-to
   nil (format ":admin-action databases restore %s"
               (sql-datum--admin-payload `((database . ,database))))))

(defun sql-datum-database-files (database)
  "Show the files making up DATABASE."
  (interactive (list (sql-datum--read-database "Files of database: ")))
  (sql-datum--admin-request
   "databases" (format ":admin-action databases files %s" database)))

;; --- Schemas ---

(defun sql-datum-new-schema ()
  "Open the wizard for creating a schema."
  (interactive)
  (sql-datum--admin-send-command-to nil ":admin-action schema new-schema"))

(defun sql-datum-drop-schema (schema)
  "Drop SCHEMA after the panel confirms what it holds."
  (interactive (list (sql-datum--read-schema "Drop schema: ")))
  (when (string-empty-p (string-trim schema))
    (user-error "No schema given"))
  (sql-datum--admin-request
   "schema" (format ":admin-action schema drop-schema %s" schema)))

;; --- Logins, roles and users ---

(defun sql-datum-new-principal ()
  "Open the wizard for creating a login or role."
  (interactive)
  (sql-datum--admin-send-command-to
   nil ":admin-action security new-principal"))

(defun sql-datum-edit-principal (name)
  "Open the wizard for altering the login or role NAME."
  (interactive (list (sql-datum--read-principal "Edit login or role: ")))
  (sql-datum--admin-send-command-to
   nil (format ":admin-action security edit-principal %s" name)))

(defun sql-datum-drop-principal (name)
  "Drop the login or role NAME, by way of its confirmation panel.
Nothing is dropped until that panel is confirmed."
  (interactive (list (sql-datum--read-principal "Drop login or role: ")))
  (sql-datum--admin-request
   "security" (format ":admin-action security drop-check %s" name)))

(defun sql-datum-permissions (name)
  "Show what the login or role NAME may do."
  (interactive (list (sql-datum--read-principal "Permissions for: ")))
  (sql-datum--admin-request
   "security" (format ":admin-action security permissions %s"
                      (sql-datum--admin-payload `((principal . ,name))))))

(defun sql-datum-user-mappings (name)
  "Show the databases the login NAME is a user in."
  (interactive (list (sql-datum--read-principal "Mappings for login: ")))
  (sql-datum--admin-request
   "security" (format ":admin-action security mappings %s" name)))

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

(defun sql-datum-admin (panel)
  "Open an admin panel.  PANEL is one of: activity, databases, jobs, ssis.
With a prefix argument, prompts for the panel name."
  (interactive
   (list (completing-read "Admin panel: "
                          '("activity" "databases" "filesystem" "jobs"
                            "schema" "security" "ssis")
                          nil t)))
  (setq sql-datum--admin-display-request panel)
  (sql-datum--send-command (format ":admin %s" panel) t))

(defun sql-datum-browse-server-files (path)
  "Browse the server's filesystem, starting at PATH.
With no PATH the server's default data directory is used."
  (interactive
   (list (when current-prefix-arg
           (read-string "Server directory: "))))
  (sql-datum--admin-request
   "filesystem" (if (and path (not (string-empty-p (string-trim path))))
                    (format ":admin filesystem %s" path)
                  ":admin filesystem")))

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
        (let ((commands '(":refresh-databases"
                          ":refresh-schemas"
                          ":refresh-tables"
                          ":refresh-routines")))
          (setq sql-datum--bg-total (length commands))
          (setq sql-datum--bg-pending (length commands))
          (sql-datum--update-mode-line)
          (sql-datum--refresh-chain commands buf))))))

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
                              'sql-datum--xdb-cache buf))))
    (when buf
      (sql-datum--refresh-async buf)
      ;; Clear column fetch tracking so new completions re-fetch
      (with-current-buffer buf
        (clrhash sql-datum--columns-fetched)))
    ;; Re-fetch any previously introspected cross-databases
    (when xdb-cache
      (let ((dbs-to-refresh nil))
        (maphash (lambda (db _entry) (push db dbs-to-refresh))
                 xdb-cache)
        (when dbs-to-refresh
          (with-current-buffer buf
            (clrhash sql-datum--xdb-cache)
            ;; Each refresh-db is one additional bg task
            (cl-incf sql-datum--bg-pending (length dbs-to-refresh))
            (cl-incf sql-datum--bg-total (length dbs-to-refresh))
            (sql-datum--update-mode-line))
          (dolist (db dbs-to-refresh)
            (sql-datum--send-command (format ":refresh-db %s" db)))))))
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
      ;; Stop any admin panel timers
      (dolist (buf (buffer-list))
        (when (and (buffer-live-p buf)
                   (string-match-p "\\*datum-admin:" (buffer-name buf)))
          (sql-datum--admin-stop-timer buf)))
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
  (define-key sql-mode-map (kbd "C-c t N") #'sql-datum-new-table)
  (define-key sql-mode-map (kbd "C-c t E") #'sql-datum-edit-table)
  (define-key sql-mode-map (kbd "C-c t S") #'sql-datum-new-schema)
  (define-key sql-mode-map (kbd "C-c t X") #'sql-datum-drop-schema)
  ;; C-c d: database operations
  (define-key sql-mode-map (kbd "C-c d N") #'sql-datum-new-database)
  (define-key sql-mode-map (kbd "C-c d E") #'sql-datum-alter-database)
  (define-key sql-mode-map (kbd "C-c d D") #'sql-datum-drop-database)
  (define-key sql-mode-map (kbd "C-c d b") #'sql-datum-backup-database)
  (define-key sql-mode-map (kbd "C-c d r") #'sql-datum-restore-database)
  (define-key sql-mode-map (kbd "C-c d f") #'sql-datum-database-files)
  ;; C-c g: logins, roles and users
  (define-key sql-mode-map (kbd "C-c g N") #'sql-datum-new-principal)
  (define-key sql-mode-map (kbd "C-c g E") #'sql-datum-edit-principal)
  (define-key sql-mode-map (kbd "C-c g D") #'sql-datum-drop-principal)
  (define-key sql-mode-map (kbd "C-c g m") #'sql-datum-user-mappings)
  (define-key sql-mode-map (kbd "C-c g P") #'sql-datum-permissions)
  ;; C-c s: session info
  (define-key sql-mode-map (kbd "C-c s p") #'sql-datum-pwd)
  (define-key sql-mode-map (kbd "C-c s t") #'sql-datum-tables)
  (define-key sql-mode-map (kbd "C-c s c") #'sql-datum-columns)
  (define-key sql-mode-map (kbd "C-c s d") #'sql-datum-databases)
  (define-key sql-mode-map (kbd "C-c s s") #'sql-datum-schemas)
  (define-key sql-mode-map (kbd "C-c s R") #'sql-datum-routines)
  (define-key sql-mode-map (kbd "C-c s r") #'sql-datum-running)
  (define-key sql-mode-map (kbd "C-c s a") #'sql-datum-admin)
  (define-key sql-mode-map (kbd "C-c s b") #'sql-datum-browse-server-files)
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
  ;; C-c s w: copy last result (w = kill-ring-save convention)
  (define-key sql-mode-map (kbd "C-c s w") #'sql-datum-copy-last-result)
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

;; Override buffer display behavior after connecting, controlled by
;; `sql-datum-connect-buffer-display'.  sql-product-interactive calls
;; sql-display-buffer which uses pop-to-buffer; we intercept it to
;; support staying in the current buffer.
(define-advice sql-display-buffer (:around (orig-fn buf)
                                           sql-datum--connect-display)
  "Respect `sql-datum-connect-buffer-display' for datum buffers."
  (if (and buf
           (with-current-buffer buf
             (and (derived-mode-p 'sql-interactive-mode)
                  (eq sql-product 'datum)))
           (not (eq sql-datum-connect-buffer-display 'pop-to-buffer)))
      (pcase sql-datum-connect-buffer-display
        ('display-buffer (display-buffer buf))
        ('nil nil))
    (funcall orig-fn buf)))

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
