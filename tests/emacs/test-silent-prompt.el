;;; test-silent-prompt.el --- Batch tests for silent command output  -*- lexical-binding: t; -*-

;; The REPL prints a prompt after every command, silent ones included.
;; Panel auto-refresh issues a silent command every few seconds, so any
;; prompt that escapes the preoutput filter lands in the SQLi buffer on
;; a timer — which is what a user sees as "> keeps printing".
;;
;; The prompt frequently arrives in a different chunk than the `ready'
;; envelope, so these tests drive the filter with the prompt split across
;; every boundary it realistically falls on.  They also pin the opposite
;; requirement: a prompt belonging to the user's own query must survive.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-silent-prompt.el

;;; Code:

(require 'cl-lib)

(defvar test-silent-prompt--pass 0)
(defvar test-silent-prompt--fail 0)

(defmacro test-silent-prompt-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-silent-prompt--pass (1+ test-silent-prompt--pass))
             (message "  PASS: %s" ,description))
         (setq test-silent-prompt--fail (1+ test-silent-prompt--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-silent-prompt--fail (1+ test-silent-prompt--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(defconst test-silent-prompt--panel
  (concat "##DATUM:admin-panel:{\"panel\":\"databases\",\"headers\":[],"
          "\"rows\":[],\"row_id\":null,\"actions\":[],\"info\":null}##")
  "An admin-panel envelope, as a panel refresh produces.")

(defun test-silent-prompt--silent (chunks)
  "Feed CHUNKS through the filter as one silent command's output.
Returns what would reach the SQLi buffer."
  (with-temp-buffer
    (setq-local sql-datum--ready t
                sql-datum--command-queue nil
                sql-datum--queue-current
                (list :commands '(":admin databases") :silent t)
                sql-datum--queue-remaining nil
                sql-datum--partial-line nil
                sql-datum--silent-prompt-pending nil
                ;; queue-send-next marks a silent command in flight.
                sql-datum--silent-in-flight t)
    (mapconcat #'sql-datum--preoutput-filter chunks "")))

(defun test-silent-prompt--interactive (chunks &optional pending)
  "Feed CHUNKS through the filter as output from the user's own query.
PENDING simulates a silent command's prompt still being owed."
  (with-temp-buffer
    (setq-local sql-datum--ready t
                sql-datum--command-queue nil
                sql-datum--queue-current nil
                sql-datum--queue-remaining nil
                sql-datum--partial-line nil
                sql-datum--silent-in-flight nil
                sql-datum--silent-prompt-pending pending)
    (mapconcat #'sql-datum--preoutput-filter chunks "")))

(message "\n=== Silent command output ===")

;; --- the prompt must never reach the buffer, wherever it is split ---
(test-silent-prompt-assert
 "everything in one chunk"
 (equal "" (test-silent-prompt--silent
            (list (concat ":admin databases\r\n"
                          test-silent-prompt--panel "\n"
                          "##DATUM:ready:##\n> ")))))

(test-silent-prompt-assert
 "prompt in its own chunk"
 (equal "" (test-silent-prompt--silent
            (list ":admin databases\r\n"
                  (concat test-silent-prompt--panel "\n")
                  "##DATUM:ready:##\n"
                  "> "))))

(test-silent-prompt-assert
 "ready and prompt share a chunk"
 (equal "" (test-silent-prompt--silent
            (list ":admin databases\r\n"
                  (concat test-silent-prompt--panel "\n")
                  "##DATUM:ready:##\n> "))))

(test-silent-prompt-assert
 "echo and envelope share a chunk, prompt separate"
 (equal "" (test-silent-prompt--silent
            (list (concat ":admin databases\r\n"
                          test-silent-prompt--panel "\n"
                          "##DATUM:ready:##\n")
                  "\n> "))))

(test-silent-prompt-assert
 "prompt split mid-sequence from the next command's echo"
 (equal "" (test-silent-prompt--silent
            (list ":admin databases\r\n"
                  (concat test-silent-prompt--panel "\n")
                  "##DATUM:ready:##\n"
                  "> :admin databases\r\n"))))

;; --- the user's own output must survive ---
(test-silent-prompt-assert
 "an ordinary prompt is left alone"
 (equal "> " (test-silent-prompt--interactive '("> "))))

(test-silent-prompt-assert
 "query output and its prompt are left alone"
 (equal "id  name\n1   foo\n\n> "
        (test-silent-prompt--interactive '("id  name\n1   foo\n\n> "))))

(test-silent-prompt-assert
 "a pending claim does not eat output that arrives first"
 (let ((out (test-silent-prompt--interactive
             '("id  name\n1   foo\n" "\n> ") t)))
   ;; The result arriving first proves the owed prompt already went by,
   ;; so the claim is dropped and the user keeps their prompt.
   (equal out "id  name\n1   foo\n\n> ")))

(test-silent-prompt-assert
 "a pending claim consumes only the first prompt"
 (equal "> " (test-silent-prompt--interactive '("> " "> ") t)))

(message "\n%d passed, %d failed"
         test-silent-prompt--pass test-silent-prompt--fail)
(when (> test-silent-prompt--fail 0)
  (kill-emacs 1))

(provide 'test-silent-prompt)
;;; test-silent-prompt.el ends here
