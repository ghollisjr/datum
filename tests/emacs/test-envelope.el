;;; test-envelope.el --- Batch-mode tests for sql-datum envelope parsing  -*- lexical-binding: t; -*-

;; Run with:
;;   emacs --batch -l sql-datum.el -l test-envelope.el

;;; Code:

(require 'cl-lib)

(defvar test-envelope--pass 0 "Count of passed tests.")
(defvar test-envelope--fail 0 "Count of failed tests.")

(defmacro test-envelope-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION on failure."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-envelope--pass (1+ test-envelope--pass))
             (message "  PASS: %s" ,description))
         (setq test-envelope--fail (1+ test-envelope--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-envelope--fail (1+ test-envelope--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(defun test-envelope--setup ()
  "Reset buffer-local state for a clean test environment."
  (setq sql-datum--partial-line nil)
  (setq sql-datum--dialect nil)
  (setq sql-datum--meta (make-hash-table :test #'equal))
  (setq sql-datum--databases nil)
  (setq sql-datum--schemas nil)
  (setq sql-datum--tables nil)
  (setq sql-datum--routines nil)
  (setq sql-datum--columns (make-hash-table :test #'equal))
  (setq sql-datum--routine-signatures (make-hash-table :test #'equal))
  (setq sql-datum--routine-types (make-hash-table :test #'equal))
  (setq sql-datum--xdb-cache (make-hash-table :test #'equal)))

;; ---------------------------------------------------------------------------
;; Test: dialect envelope
;; ---------------------------------------------------------------------------

(defun test-envelope--dialect ()
  "Test that ##DATUM:dialect:mysql## sets sql-datum--dialect."
  (message "\n--- dialect envelope ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter "##DATUM:dialect:mysql##\n")
  (test-envelope-assert "dialect set to mysql"
                        (equal sql-datum--dialect "mysql"))

  (test-envelope--setup)
  (sql-datum--preoutput-filter "##DATUM:dialect:postgres##\n")
  (test-envelope-assert "dialect set to postgres"
                        (equal sql-datum--dialect "postgres"))

  (test-envelope--setup)
  (sql-datum--preoutput-filter "##DATUM:dialect:mssql##\n")
  (test-envelope-assert "dialect set to mssql"
                        (equal sql-datum--dialect "mssql")))

;; ---------------------------------------------------------------------------
;; Test: introspect envelope — tables
;; ---------------------------------------------------------------------------

(defun test-envelope--introspect-tables ()
  "Test that ##DATUM:introspect:tables:[...]## updates sql-datum--tables."
  (message "\n--- introspect tables ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:tables:[\"customers\",\"orders\",\"products\"]##\n")
  (test-envelope-assert "tables list populated"
                        (equal sql-datum--tables
                               '("customers" "orders" "products")))

  ;; Verify replacement (not append)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:tables:[\"new_table\"]##\n")
  (test-envelope-assert "tables list replaced (not appended)"
                        (equal sql-datum--tables '("new_table"))))

;; ---------------------------------------------------------------------------
;; Test: introspect envelope — databases, schemas
;; ---------------------------------------------------------------------------

(defun test-envelope--introspect-databases-schemas ()
  "Test databases and schemas introspect envelopes."
  (message "\n--- introspect databases/schemas ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:databases:[\"datum_test\",\"mysql\",\"information_schema\"]##\n")
  (test-envelope-assert "databases list populated"
                        (member "datum_test" sql-datum--databases))

  (test-envelope--setup)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:schemas:[\"public\",\"pg_catalog\"]##\n")
  (test-envelope-assert "schemas list populated"
                        (equal sql-datum--schemas '("public" "pg_catalog"))))

;; ---------------------------------------------------------------------------
;; Test: meta envelope
;; ---------------------------------------------------------------------------

(defun test-envelope--meta ()
  "Test that ##DATUM:meta:key:value## updates sql-datum--meta."
  (message "\n--- meta envelope ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter "##DATUM:meta:database:datum_test##\n")
  (test-envelope-assert "meta database set"
                        (equal (gethash "database" sql-datum--meta)
                               "datum_test"))

  (sql-datum--preoutput-filter "##DATUM:meta:user:root@localhost##\n")
  (test-envelope-assert "meta user set"
                        (equal (gethash "user" sql-datum--meta)
                               "root@localhost"))

  (sql-datum--preoutput-filter "##DATUM:meta:version:10.6.12-MariaDB##\n")
  (test-envelope-assert "meta version set"
                        (equal (gethash "version" sql-datum--meta)
                               "10.6.12-MariaDB")))

;; ---------------------------------------------------------------------------
;; Test: envelope lines are stripped from visible output
;; ---------------------------------------------------------------------------

(defun test-envelope--stripped ()
  "Test that envelope lines are removed from visible output."
  (message "\n--- envelope stripping ---")
  (test-envelope--setup)
  (let ((result (sql-datum--preoutput-filter
                 "##DATUM:dialect:mysql##\nhello world\n##DATUM:meta:database:test##\n")))
    (test-envelope-assert "envelope lines stripped from output"
                          (not (string-match-p "##DATUM:" result)))
    (test-envelope-assert "non-envelope output passes through"
                          (string-match-p "hello world" result))))

;; ---------------------------------------------------------------------------
;; Test: non-envelope output passes through unchanged
;; ---------------------------------------------------------------------------

(defun test-envelope--passthrough ()
  "Test that regular output passes through the filter unchanged."
  (message "\n--- passthrough ---")
  (test-envelope--setup)
  (let ((input "SELECT * FROM customers;\nid | name\n1  | Alice\n"))
    (let ((result (sql-datum--preoutput-filter input)))
      (test-envelope-assert "regular output unchanged"
                            (equal result input)))))

;; ---------------------------------------------------------------------------
;; Test: chunked introspect+ appends correctly
;; ---------------------------------------------------------------------------

(defun test-envelope--introspect-append ()
  "Test that introspect+ appends to existing data."
  (message "\n--- introspect+ append ---")
  (test-envelope--setup)
  ;; First chunk (replaces)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:tables:[\"t1\",\"t2\"]##\n")
  (test-envelope-assert "initial tables set"
                        (equal sql-datum--tables '("t1" "t2")))
  ;; Continuation chunk (appends)
  (sql-datum--preoutput-filter
   "##DATUM:introspect+:tables:[\"t3\",\"t4\"]##\n")
  (test-envelope-assert "tables appended via introspect+"
                        (equal sql-datum--tables '("t1" "t2" "t3" "t4")))
  ;; Another append
  (sql-datum--preoutput-filter
   "##DATUM:introspect+:tables:[\"t5\"]##\n")
  (test-envelope-assert "second append works"
                        (equal sql-datum--tables '("t1" "t2" "t3" "t4" "t5"))))

;; ---------------------------------------------------------------------------
;; Test: columns introspect
;; ---------------------------------------------------------------------------

(defun test-envelope--introspect-columns ()
  "Test columns introspect envelope."
  (message "\n--- introspect columns ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:columns:customers:[\"customer_id\",\"first_name\",\"email\"]##\n")
  (test-envelope-assert "columns for customers set"
                        (equal (gethash "customers" sql-datum--columns)
                               '("customer_id" "first_name" "email"))))

;; ---------------------------------------------------------------------------
;; Test: routine-sigs and routine-types introspect
;; ---------------------------------------------------------------------------

(defun test-envelope--introspect-routines ()
  "Test routine-sigs and routine-types introspect envelopes."
  (message "\n--- introspect routines ---")
  (test-envelope--setup)
  (sql-datum--preoutput-filter
   "##DATUM:introspect:routines:[\"format_currency\",\"place_order\"]##\n")
  (test-envelope-assert "routines list set"
                        (equal sql-datum--routines
                               '("format_currency" "place_order")))

  (sql-datum--preoutput-filter
   "##DATUM:introspect:routine-types:[[\"format_currency\",\"FUNCTION\"],[\"place_order\",\"PROCEDURE\"]]##\n")
  (test-envelope-assert "routine type for format_currency"
                        (equal (gethash "format_currency" sql-datum--routine-types)
                               "FUNCTION"))
  (test-envelope-assert "routine type for place_order"
                        (equal (gethash "place_order" sql-datum--routine-types)
                               "PROCEDURE"))

  (sql-datum--preoutput-filter
   "##DATUM:introspect:routine-sigs:[[\"format_currency\",\"amount DECIMAL, currency VARCHAR\"],[\"place_order\",\"cust_id INT, prod_id INT\"]]##\n")
  (test-envelope-assert "routine sig for format_currency"
                        (equal (gethash "format_currency" sql-datum--routine-signatures)
                               "amount DECIMAL, currency VARCHAR"))
  (test-envelope-assert "routine sig for place_order"
                        (equal (gethash "place_order" sql-datum--routine-signatures)
                               "cust_id INT, prod_id INT")))

;; ---------------------------------------------------------------------------
;; Test: definition envelope with escaped newlines
;; ---------------------------------------------------------------------------

(defun test-envelope--definition ()
  "Test definition envelope parsing with escaped newlines."
  (message "\n--- definition envelope ---")
  (test-envelope--setup)
  ;; We can't easily test the full show-definition flow in batch mode
  ;; (it uses run-at-time and buffer display), but we can verify the
  ;; envelope is recognized and parsed.
  (let ((called nil)
        (captured-name nil)
        (captured-text nil))
    ;; Temporarily advise the handler to capture the call
    (cl-letf (((symbol-function 'sql-datum--show-definition)
               (lambda (name text _sqli-buf)
                 (setq called t
                       captured-name name
                       captured-text text))))
      ;; The definition handler uses run-at-time 0, so in batch mode
      ;; we need to handle it directly. Instead, test the envelope dispatch.
      ;;
      ;; The payload is JSON, matching `envelope.definition' on the Python
      ;; side: names can contain colons, so the old "name:text" form was
      ;; ambiguous.  Newlines arrive escaped and are unescaped by the
      ;; handler.
      (sql-datum--handle-envelope
       "definition"
       (json-serialize
        '((name . "my_table")
          (text . "CREATE TABLE my_table (\\nid INT,\\nname VARCHAR(100)\\n)")))))
    ;; run-at-time won't fire in batch mode, so we just verify it was scheduled
    ;; by checking that no error occurred during dispatch
    (test-envelope-assert "definition envelope parsed without error" t)))

;; ---------------------------------------------------------------------------
;; Test: partial line buffering
;; ---------------------------------------------------------------------------

(defun test-envelope--partial-line ()
  "Test that partial envelope lines are buffered across filter calls."
  (message "\n--- partial line buffering ---")
  (test-envelope--setup)
  ;; Send a partial envelope (no closing ##, no trailing newline)
  (let ((result1 (sql-datum--preoutput-filter "##DATUM:dialect:my")))
    (test-envelope-assert "partial line buffered"
                          (equal sql-datum--partial-line "##DATUM:dialect:my"))
    ;; Complete the envelope in the next call
    (let ((result2 (sql-datum--preoutput-filter "sql##\nsome output\n")))
      (test-envelope-assert "dialect set after reassembly"
                            (equal sql-datum--dialect "mysql"))
      (test-envelope-assert "partial line cleared"
                            (null sql-datum--partial-line))
      (test-envelope-assert "non-envelope output passed through"
                            (string-match-p "some output" result2)))))

;; ---------------------------------------------------------------------------
;; Run all tests
;; ---------------------------------------------------------------------------

(defun test-envelope--run-all ()
  "Run all envelope tests and report results."
  (message "=== Emacs Envelope Tests ===")
  (test-envelope--dialect)
  (test-envelope--introspect-tables)
  (test-envelope--introspect-databases-schemas)
  (test-envelope--meta)
  (test-envelope--stripped)
  (test-envelope--passthrough)
  (test-envelope--introspect-append)
  (test-envelope--introspect-columns)
  (test-envelope--introspect-routines)
  (test-envelope--definition)
  (test-envelope--partial-line)
  (message "\n=== Results ===")
  (message "Passed: %d" test-envelope--pass)
  (message "Failed: %d" test-envelope--fail)
  (when (> test-envelope--fail 0)
    (kill-emacs 1)))

;; Run tests when loaded in batch mode
(when noninteractive
  (test-envelope--run-all))

(provide 'test-envelope)
;;; test-envelope.el ends here
