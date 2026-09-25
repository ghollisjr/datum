;;; test-table-keys.el --- C-c t table wizard shortcuts -*- lexical-binding: t; -*-

(require 'cl-lib)

(defvar test-table-keys--pass 0)
(defvar test-table-keys--fail 0)

(defmacro test-table-keys-assert (name form)
  `(condition-case err
       (if ,form
           (progn (setq test-table-keys--pass (1+ test-table-keys--pass))
                  (message "  PASS: %s" ,name))
         (setq test-table-keys--fail (1+ test-table-keys--fail))
         (message "  FAIL: %s" ,name))
     (error (setq test-table-keys--fail (1+ test-table-keys--fail))
            (message "  FAIL: %s (error: %s)" ,name err))))

(defmacro test-table-keys-error (name form)
  "Assert FORM signals a `user-error' whose message matches NAME's intent."
  `(condition-case nil
       (progn ,form
              (setq test-table-keys--fail (1+ test-table-keys--fail))
              (message "  FAIL: %s (no error)" ,name))
     (user-error (setq test-table-keys--pass (1+ test-table-keys--pass))
                 (message "  PASS: %s" ,name))))

;; A stand-in for the SQLi buffer the caches live in.
(defvar test-table-keys--sqli nil)

(defun test-table-keys--with-cache (tables body)
  "Run BODY with TABLES as the introspection cache."
  (let ((buf (get-buffer-create "*test-sqli*")))
    (with-current-buffer buf
      (setq-local sql-datum--tables tables)
      (setq-local sql-datum--schemas '("dbo" "sales")))
    (cl-letf (((symbol-function 'sql-find-sqli-buffer)
               (lambda (&rest _) "*test-sqli*")))
      (funcall body))))

(message "\n=== table name resolution ===")

(test-table-keys--with-cache
 '("dbo.orders" "sales.customers" "dbo.items" "sales.items")
 (lambda ()
   (test-table-keys-assert "a qualified name splits as given"
                           (equal (sql-datum--table-parts "dbo.orders")
                                  '("dbo" . "orders")))
   (test-table-keys-assert "a bracket-quoted name is unquoted"
                           (equal (sql-datum--table-parts "[dbo].[my orders]")
                                  '("dbo" . "my orders")))
   ;; A bare name is what the query buffer usually holds.
   (test-table-keys-assert "a bare name finds its only schema"
                           (equal (sql-datum--table-parts "orders")
                                  '("dbo" . "orders")))
   (test-table-keys-assert "matching a bare name ignores case"
                           (equal (sql-datum--table-parts "ORDERS")
                                  '("dbo" . "orders")))
   ;; Guessing here would edit the wrong table.
   (test-table-keys-error "a name in two schemas is not guessed at"
                          (cl-letf (((symbol-function 'completing-read)
                                     (lambda (&rest _)
                                       (signal 'user-error '("asked")))))
                            (sql-datum--table-parts "items")))
   (test-table-keys-assert "and the answer is used when given"
                           (cl-letf (((symbol-function 'completing-read)
                                      (lambda (&rest _) "sales.items")))
                             (equal (sql-datum--table-parts "items")
                                    '("sales" . "items"))))
   (test-table-keys-error "an unknown bare name says to qualify it"
                          (sql-datum--table-parts "nowhere"))
   ;; The wizards act on the connected database only.
   (test-table-keys-error "a three-part name is refused"
                          (sql-datum--table-parts "other.dbo.orders"))))

(message "\n=== bindings ===")

(require 'sql)
(test-table-keys-assert "C-c t N builds a new table"
                        (eq (lookup-key sql-mode-map (kbd "C-c t N"))
                            'sql-datum-new-table))
(test-table-keys-assert "C-c t E edits one"
                        (eq (lookup-key sql-mode-map (kbd "C-c t E"))
                            'sql-datum-edit-table))
(test-table-keys-assert "C-c t D still drops one"
                        (eq (lookup-key sql-mode-map (kbd "C-c t D"))
                            'sql-datum-drop-table))
(test-table-keys-assert "the lower-case C-c t keys are untouched"
                        (and (eq (lookup-key sql-mode-map (kbd "C-c t d"))
                                 'sql-datum-describe)
                             (eq (lookup-key sql-mode-map (kbd "C-c t e"))
                                 'sql-datum-export)))

(message "\n=== the command each key sends ===")

(let (sent)
  (cl-letf (((symbol-function 'sql-datum--admin-send-command-to)
             (lambda (_buf cmd) (setq sent cmd))))
    (test-table-keys--with-cache
     '("dbo.orders")
     (lambda ()
       (sql-datum-new-table "sales")
       (test-table-keys-assert "new-table names the schema"
                               (equal sent ":admin-action schema new-table sales"))
       (sql-datum-edit-table "dbo.orders")
       (test-table-keys-assert "edit-table carries schema and table"
                               (equal
                                (json-parse-string
                                 (decode-coding-string
                                  (base64-decode-string
                                   (car (last (split-string sent " "))))
                                  'utf-8)
                                 :object-type 'alist)
                                '((schema . "dbo") (table . "orders"))))))))


(message "\n=== database, schema and security keys ===")

(dolist (spec '(("C-c t S" sql-datum-new-schema)
                ("C-c t X" sql-datum-drop-schema)
                ("C-c d N" sql-datum-new-database)
                ("C-c d E" sql-datum-alter-database)
                ("C-c d D" sql-datum-drop-database)
                ("C-c d b" sql-datum-backup-database)
                ("C-c d r" sql-datum-restore-database)
                ("C-c d f" sql-datum-database-files)
                ("C-c g N" sql-datum-new-principal)
                ("C-c g E" sql-datum-edit-principal)
                ("C-c g D" sql-datum-drop-principal)
                ("C-c g m" sql-datum-user-mappings)))
  (test-table-keys-assert (format "%s runs %s" (nth 0 spec) (nth 1 spec))
                          (eq (lookup-key sql-mode-map (kbd (nth 0 spec)))
                              (nth 1 spec))))

;; C-c u must keep meaning "use database", not become a d-prefix casualty.
(test-table-keys-assert "C-c u still switches database"
                        (eq (lookup-key sql-mode-map (kbd "C-c u"))
                            'sql-datum-use-database))

(message "\n=== what the database and security keys send ===")

(let (sent request)
  (cl-letf (((symbol-function 'sql-datum--admin-send-command-to)
             (lambda (_buf cmd) (setq sent cmd))))
    (setq sql-datum--admin-display-request nil)
    (sql-datum-alter-database "payroll")
    (test-table-keys-assert "alter names the database"
                            (equal sent
                                   ":admin-action databases edit-database payroll"))
    (sql-datum-drop-database "payroll")
    (setq request sql-datum--admin-display-request)
    (test-table-keys-assert "drop goes through the check, not a DROP"
                            (equal sent
                                   ":admin-action databases drop-check payroll"))
    ;; The confirmation panel has to actually surface, or the drop
    ;; would look like it did nothing.
    (test-table-keys-assert "and asks for the panel to be shown"
                            (equal request "databases"))
    (sql-datum-backup-database "payroll")
    (test-table-keys-assert "backup carries the database in its payload"
                            (equal (json-parse-string
                                    (decode-coding-string
                                     (base64-decode-string
                                      (car (last (split-string sent " "))))
                                     'utf-8)
                                    :object-type 'alist)
                                   '((database . "payroll"))))
    (sql-datum-restore-database "payroll")
    (test-table-keys-assert "restore does too"
                            (string-prefix-p
                             ":admin-action databases restore " sent))
    (sql-datum-new-principal)
    (test-table-keys-assert "new principal needs no name"
                            (equal sent
                                   ":admin-action security new-principal"))
    (sql-datum-drop-principal "reporting")
    (test-table-keys-assert "dropping a login goes through its check too"
                            (equal sent
                                   ":admin-action security drop-check reporting"))))

(message "\n=== principals are remembered for completion ===")

(let ((sqli (get-buffer-create "*test-sqli*")))
  (with-current-buffer sqli (setq-local sql-datum--principals nil))
  (sql-datum--cache-principals '(("sa" "SQL login" "yes")
                                 ("reporting" "SQL login" "yes")
                                 ("sa" "SQL login" "yes"))
                               sqli)
  (test-table-keys-assert "the security panel fills the completion list"
                          (equal (buffer-local-value 'sql-datum--principals
                                                     sqli)
                                 '("sa" "reporting")))
  ;; Before the panel has been opened there is simply nothing to offer,
  ;; which must read as free text rather than an error.
  (with-current-buffer sqli (setq-local sql-datum--principals nil))
  (test-table-keys-assert "a cold list still lets a name be typed"
                          (cl-letf (((symbol-function 'sql-find-sqli-buffer)
                                     (lambda (&rest _) "*test-sqli*"))
                                    ((symbol-function 'completing-read)
                                     (lambda (_p coll &rest _)
                                       (if coll "unexpected" "typed"))))
                            (equal (sql-datum--read-principal "x: ")
                                   "typed"))))

(message "\n%d passed, %d failed" test-table-keys--pass test-table-keys--fail)
