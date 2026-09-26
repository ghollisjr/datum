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


(message "\n=== permissions keys ===")

(test-table-keys-assert "C-c g P shows permissions"
                        (eq (lookup-key sql-mode-map (kbd "C-c g P"))
                            'sql-datum-permissions))
(dolist (spec '(("P" sql-datum-admin-permissions)
                ("G" sql-datum-admin-grant)
                ("R" sql-datum-admin-restore-or-revoke)
                ;; U is shared with the filesystem listing, where it
                ;; unmarks; the dispatcher keeps this meaning here.
                ("U" sql-datum-admin-unmark-or-mappings)))
  (test-table-keys-assert
   (format "panel key %s runs %s" (nth 0 spec) (nth 1 spec))
   (eq (lookup-key sql-datum--admin-mode-map (nth 0 spec)) (nth 1 spec))))

;; R already meant restore in the backups view; it must keep doing so.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "databases")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "backups")))
  (let (called)
    (cl-letf (((symbol-function 'sql-datum-admin-restore)
               (lambda () (setq called 'restore)))
              ((symbol-function 'sql-datum-admin-revoke-permission)
               (lambda () (setq called 'revoke))))
      (sql-datum-admin-restore-or-revoke)
      (test-table-keys-assert "R still restores in the backups view"
                              (eq called 'restore)))))

(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "permissions")))
  (let (called)
    (cl-letf (((symbol-function 'sql-datum-admin-restore)
               (lambda () (setq called 'restore)))
              ((symbol-function 'sql-datum-admin-revoke-permission)
               (lambda () (setq called 'revoke))))
      (sql-datum-admin-restore-or-revoke)
      (test-table-keys-assert "and revokes in the permissions view"
                              (eq called 'revoke)))))


;; The shared keys must keep their old meaning outside the listing.
(dolist (probe '(("U" sql-datum-admin-user-mappings "security")
                 ("o" sql-datum-admin-sort "databases")))
  (with-temp-buffer
    (setq-local sql-datum--admin-panel-name (nth 2 probe))
    (let (called)
      (cl-letf (((symbol-function (nth 1 probe))
                 (lambda (&rest _) (setq called t)))
                ((symbol-function 'sql-datum-admin-fs-unmark-all)
                 (lambda () (setq called 'marks)))
                ((symbol-function 'sql-datum-admin-open-locally)
                 (lambda () (setq called 'open))))
        (call-interactively (lookup-key sql-datum--admin-mode-map
                                        (nth 0 probe)))
        (test-table-keys-assert
         (format "%s still means %s in the %s panel"
                 (nth 0 probe) (nth 1 probe) (nth 2 probe))
         (eq called t))))))

(message "\n=== which principal and database the panel is about ===")

;; From the principal list the row names the principal, server-wide.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . nil)))
  (cl-letf (((symbol-function 'sql-datum--admin-row-id-at-point)
             (lambda () "app_login")))
    (test-table-keys-assert "a login row means that login, no database"
                            (equal (sql-datum--admin-permission-context)
                                   '("app_login" nil)))))

;; From a user-mapping row the database is the row, the login the context.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "user-mappings")))
  (setq-local sql-datum--admin-context '((login . "app_login")))
  (cl-letf (((symbol-function 'sql-datum--admin-row-id-at-point)
             (lambda () "payroll")))
    (test-table-keys-assert "a mapping row means that login in that database"
                            (equal (sql-datum--admin-permission-context)
                                   '("app_login" "payroll")))))

;; Inside the permissions view both come from the context it carries.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "permissions")))
  (setq-local sql-datum--admin-context
              '((principal . "app_login") (database . "payroll")))
  (test-table-keys-assert "the permissions view remembers both"
                          (equal (sql-datum--admin-permission-context)
                                 '("app_login" "payroll"))))

;; An empty database string means server level, not a database named "".
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "permissions")))
  (setq-local sql-datum--admin-context
              '((principal . "app_login") (database . "")))
  (test-table-keys-assert "an empty database reads as server level"
                          (equal (sql-datum--admin-permission-context)
                                 '("app_login" nil))))

(message "\n=== what the permission keys send ===")

(let (sent)
  (cl-letf (((symbol-function 'sql-datum--admin-send-command)
             (lambda (cmd) (setq sent cmd))))
    (with-temp-buffer
      (setq-local sql-datum--admin-panel-name "security")
      (setq-local sql-datum--admin-panel-data '((sub_panel . "user-mappings")))
      (setq-local sql-datum--admin-context '((login . "app_login")))
      (cl-letf (((symbol-function 'sql-datum--admin-row-id-at-point)
                 (lambda () "payroll")))
        (sql-datum-admin-permissions))
      (test-table-keys-assert "P carries the login and the database"
                              (equal (json-parse-string
                                      (decode-coding-string
                                       (base64-decode-string
                                        (car (last (split-string sent " "))))
                                       'utf-8)
                                      :object-type 'alist)
                                     '((principal . "app_login")
                                       (database . "payroll")))))
    ;; Revoking must name the exact row, column grants included.
    (with-temp-buffer
      (setq-local sql-datum--admin-panel-name "security")
      (setq-local sql-datum--admin-panel-data '((sub_panel . "permissions")))
      (setq-local sql-datum--admin-context
                  '((principal . "app_login") (database . "payroll")))
      (cl-letf (((symbol-function 'sql-datum--admin-row-cells-at-point)
                 (lambda () '("GRANT" "SELECT" "Object"
                              "dbo.customers" "email")))
                ((symbol-function 'yes-or-no-p) (lambda (_) t)))
        (sql-datum-admin-revoke-permission))
      (test-table-keys-assert "R names the row it is revoking"
                              (equal (json-parse-string
                                      (decode-coding-string
                                       (base64-decode-string
                                        (car (last (split-string sent " "))))
                                       'utf-8)
                                      :object-type 'alist)
                                     '((principal . "app_login")
                                       (permission . "SELECT")
                                       (scope . "Object")
                                       (securable . "dbo.customers")
                                       (column . "email")
                                       (database . "payroll")))))
    ;; Nothing is revoked without agreement.
    (with-temp-buffer
      (setq-local sql-datum--admin-panel-name "security")
      (setq-local sql-datum--admin-panel-data '((sub_panel . "permissions")))
      (setq-local sql-datum--admin-context '((principal . "app_login")))
      (setq sent nil)
      (cl-letf (((symbol-function 'sql-datum--admin-row-cells-at-point)
                 (lambda () '("GRANT" "SELECT" "Object" "dbo.t" "")))
                ((symbol-function 'yes-or-no-p) (lambda (_) nil)))
        (sql-datum-admin-revoke-permission))
      (test-table-keys-assert "declining the prompt revokes nothing"
                              (null sent)))))

;; Granting belongs to the permissions view, where the principal is known.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "security")
  (setq-local sql-datum--admin-panel-data '((sub_panel . nil)))
  (test-table-keys-error "G outside the permissions view says where to go"
                         (sql-datum-admin-grant)))


(message "\n=== the jobs list can make and change a job ===")

;; Steps and schedules already had editors; the job they belong to did
;; not, so E, N and D in the list are the job's own properties.
(let (sent)
  (cl-letf (((symbol-function 'sql-datum--admin-send-command)
             (lambda (c) (setq sent c))))
    (dolist (probe '((sql-datum-admin-new-job "new-job" nil)
                     (sql-datum-admin-edit-job "edit-job" t)
                     (sql-datum-admin-delete-job "drop-job-check" t)))
      (with-temp-buffer
        (setq-local sql-datum--admin-panel-name "jobs")
        (setq-local sql-datum--admin-panel-data '((sub_panel . nil)))
        (setq sent nil)
        (cl-letf (((symbol-function 'sql-datum--admin-row-id-at-point)
                   (lambda () "nightly load")))
          (funcall (nth 0 probe)))
        (test-table-keys-assert
         (format "%s asks for %s" (nth 0 probe) (nth 1 probe))
         (string-match-p (regexp-quote (nth 1 probe)) (or sent "")))
        (when (nth 2 probe)
          (test-table-keys-assert
           (format "%s names the job" (nth 0 probe))
           (string-suffix-p "nightly load" sent)))))))

;; Deleting goes through the panel that says what would go with it.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "jobs")
  (setq-local sql-datum--admin-panel-data '((sub_panel . nil)))
  (let (sent)
    (cl-letf (((symbol-function 'sql-datum--admin-send-command)
               (lambda (c) (setq sent c)))
              ((symbol-function 'sql-datum--admin-row-id-at-point)
               (lambda () "nightly load")))
      (sql-datum-admin-delete-job)
      (test-table-keys-assert "D checks before deleting a job"
                              (string-match-p "drop-job-check" sent)))))

;; The dispatchers reach them, and the section keys still win inside the
;; detail view where steps and schedules live.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "jobs")
  (setq-local sql-datum--admin-panel-data '((sub_panel . nil)))
  (let (called)
    (cl-letf (((symbol-function 'sql-datum-admin-edit-job)
               (lambda () (setq called 'job)))
              ((symbol-function 'sql-datum-admin-edit-step)
               (lambda () (setq called 'step))))
      (sql-datum-admin-edit-at-point)
      (test-table-keys-assert "E in the jobs list edits the job"
                              (eq called 'job)))))

(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "jobs")
  (setq-local sql-datum--admin-panel-data '((sub_panel . "detail")))
  (insert "a step row\n")
  (put-text-property (point-min) (point-max) 'sql-datum-section "Steps")
  (goto-char (point-min))
  (let (called)
    (cl-letf (((symbol-function 'sql-datum-admin-edit-job)
               (lambda () (setq called 'job)))
              ((symbol-function 'sql-datum-admin-edit-step)
               (lambda () (setq called 'step))))
      (sql-datum-admin-edit-at-point)
      (test-table-keys-assert "but a step row still edits the step"
                              (eq called 'step)))))

;; Outside the jobs panel they say so rather than doing nothing.
(with-temp-buffer
  (setq-local sql-datum--admin-panel-name "databases")
  (test-table-keys-error "new-job elsewhere says where it belongs"
                         (sql-datum-admin-new-job)))

(message "\n%d passed, %d failed" test-table-keys--pass test-table-keys--fail)
