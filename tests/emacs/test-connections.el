;;; test-connections.el --- Batch tests for switching between connections  -*- lexical-binding: t; -*-

;; `sql-product-interactive' has two paths.  Starting a session points
;; the buffer it was called from at the new session; finding one already
;; running shows it and does neither — so asking to connect to a server
;; you are already connected to looked like it worked and changed
;; nothing, and statements went on going to the old connection.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-connections.el

;;; Code:

(require 'cl-lib)
(require 'sql)

(defvar test-connections--pass 0)
(defvar test-connections--fail 0)

(defmacro test-connections-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-connections--pass (1+ test-connections--pass))
             (message "  PASS: %s" ,description))
         (setq test-connections--fail (1+ test-connections--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-connections--fail (1+ test-connections--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(defun test-connections--sqli (name)
  "Return a stand-in SQLi buffer for the connection called NAME."
  (let ((buf (get-buffer-create (format "*SQL: <%s>*" name))))
    (with-current-buffer buf
      (sql-interactive-mode)
      (setq-local sql-product 'datum)
      (setq-local sql-connection name)
      (setq-local sql-buffer (buffer-name buf)))
    (setq-default sql-buffer (buffer-name buf))
    buf))

(defmacro test-connections--with-live-processes (&rest body)
  "Run BODY with the stand-in SQLi buffers looking like live sessions."
  `(cl-letf (((symbol-function 'comint-check-proc)
              (lambda (b) (and b (string-prefix-p
                                  "*SQL: " (if (bufferp b) (buffer-name b)
                                             (format "%s" b)))
                               t)))
             ((symbol-function 'get-buffer-process)
              (lambda (b) (and b (string-prefix-p
                                  "*SQL: " (if (bufferp b) (buffer-name b)
                                             (format "%s" b)))
                               t))))
     ,@body))

(message "\n=== connecting to a connection already open ===")

(let ((one (test-connections--sqli "one"))
      (two (test-connections--sqli "two"))
      (work (get-buffer-create "test-connections-work.sql")))
  (setq sql-connection-alist
        '(("one" (sql-product 'datum) (sql-server "server-ONE"))
          ("two" (sql-product 'datum) (sql-server "server-TWO"))))
  (with-current-buffer work
    (sql-mode)
    (setq-local sql-buffer (buffer-name two)))

  (test-connections--with-live-processes
   (let (displayed)
     (cl-letf (((symbol-function 'sql-display-buffer)
                (lambda (b) (setq displayed b))))
       (with-current-buffer work (sql-connect "one")))
     ;; sql.el finds and shows the running session; what it does not do
     ;; is point this buffer at it.
     (test-connections-assert "the running session is the one shown"
                              (equal displayed (buffer-name one)))
     (test-connections-assert "and this buffer now sends to it"
                              (equal (buffer-local-value 'sql-buffer work)
                                     (buffer-name one)))
     (test-connections-assert "as does anything with no connection of its own"
                              (equal (default-value 'sql-buffer)
                                     (buffer-name one)))

     ;; Asking again for the one already current changes nothing and says
     ;; nothing.
     (setq displayed nil)
     (cl-letf (((symbol-function 'sql-display-buffer)
                (lambda (b) (setq displayed b))))
       (with-current-buffer work (sql-connect "one")))
     (test-connections-assert "asking for the current one is a no-op"
                              (equal (buffer-local-value 'sql-buffer work)
                                     (buffer-name one)))

     ;; And back the other way, which is the whole point of the flow.
     (cl-letf (((symbol-function 'sql-display-buffer) (lambda (_b) nil)))
       (with-current-buffer work (sql-connect "two")))
     (test-connections-assert "switching back works the same way"
                              (equal (buffer-local-value 'sql-buffer work)
                                     (buffer-name two)))))

  ;; Turning it off leaves sql.el's behaviour exactly as it was.
  (with-current-buffer work (setq-local sql-buffer (buffer-name two)))
  (test-connections--with-live-processes
   (let ((sql-datum-connect-adopts-existing nil))
     (cl-letf (((symbol-function 'sql-display-buffer) (lambda (_b) nil)))
       (with-current-buffer work (sql-connect "one")))
     (test-connections-assert "with the option off, nothing is adopted"
                              (equal (buffer-local-value 'sql-buffer work)
                                     (buffer-name two)))))

  ;; A buffer that is not `sql-mode' is left alone, the same restriction
  ;; sql.el puts on the case where it does start a session.
  (let ((plain (get-buffer-create "test-connections-plain.txt")))
    (with-current-buffer plain (fundamental-mode))
    (test-connections--with-live-processes
     (cl-letf (((symbol-function 'sql-display-buffer) (lambda (_b) nil)))
       (with-current-buffer plain (sql-connect "one")))
     (test-connections-assert "a non-SQL buffer is not repointed"
                              (not (local-variable-p 'sql-buffer plain))))
    (kill-buffer plain))

  ;; Starting a session that is genuinely new must go through untouched:
  ;; sql.el points the buffer at it itself, and the advice must not
  ;; second-guess that.
  (with-current-buffer work (setq-local sql-buffer (buffer-name two)))
  (test-connections--with-live-processes
   (let ((started nil))
     (cl-letf (((symbol-function 'sql-find-sqli-buffer)
                ;; Nothing running for this connection.
                (lambda (&rest _) nil))
               ((symbol-function 'sql-get-login) (lambda (&rest _) nil))
               ((symbol-function 'sql-comint-datum)
                (lambda (&rest _)
                  (setq started t)
                  ;; What the comint func leaves behind.
                  (let ((new (get-buffer-create "*SQL: <three>*")))
                    (with-current-buffer new (sql-interactive-mode)
                                             (setq-local sql-product 'datum))
                    (set-buffer new))))
               ((symbol-function 'sql-display-buffer) (lambda (_b) nil)))
       (condition-case nil
           (with-current-buffer work (sql-connect "one"))
         (error nil)))
     (test-connections-assert "a new session is still started"
                              started)))
  (dolist (b (list one two work (get-buffer "*SQL: <three>*")))
    (when (buffer-live-p b) (kill-buffer b))))

(message "\n%d passed, %d failed" test-connections--pass test-connections--fail)
(when (> test-connections--fail 0)
  (kill-emacs 1))

(provide 'test-connections)
;;; test-connections.el ends here
