;;; docker-connections.el --- sql-connection-alist entries for the docker test DBs  -*- lexical-binding: t; -*-

;; Connections to the test databases defined in docker-compose.yml, for
;; manual testing in Emacs.
;;
;; Start the servers first:
;;
;;     docker compose up -d postgres mssql mssql-init
;;     docker compose up -d mysql                      ; optional
;;     docker compose --profile oracle up -d oracle    ; optional
;;
;; Then load this file from your init.el:
;;
;;     (load "/path/to/datum/tests/emacs/docker-connections.el")
;;
;; or, inside a `use-package sql-datum' :config block, just evaluate it
;; after sql-datum is loaded.  Connect with C-c C-x c (or C-u C-c C-x c
;; for a second, concurrent connection to the same server).
;;
;; The credentials below are the ones committed in docker-compose.yml.
;; They are local test fixtures, not secrets, which is why they are
;; inline here rather than going through auth-source.

;;; Code:

(require 'sql)

;; Host ports are remapped by docker-compose to avoid colliding with any
;; locally installed server:
;;
;;   postgres  5433 -> 5432
;;   mssql     1434 -> 1433
;;   mariadb   3307 -> 3306
;;   oracle    1522 -> 1521
;;
;; datum picks the dialect from the Driver= name, so no --sql-type is
;; needed.  Run `odbcinst -q -d' if a driver name below does not match
;; what is installed locally.

(dolist
    (entry
     `(("docker-pg"
        ;; PostgreSQL takes the port as its own keyword.
        "Driver={PostgreSQL Unicode};Server=127.0.0.1;Port=5433;Database=datum_test;Uid=postgres;Pwd=datum_test")

       ("docker-pg-admin"
        ;; Connects to `postgres' instead of `datum_test' so the database
        ;; wizard can create and drop datum_test itself — you cannot drop
        ;; the database you are connected to.
        "Driver={PostgreSQL Unicode};Server=127.0.0.1;Port=5433;Database=postgres;Uid=postgres;Pwd=datum_test")

       ("docker-mssql"
        ;; MSSQL takes the port as Server=host,port.  Driver 18 defaults
        ;; to Encrypt=yes, so the container's self-signed certificate
        ;; needs TrustServerCertificate=yes.  For Driver 17, use
        ;; Encrypt=No instead.
        ;;
        ;; The datum_test database is created by the mssql-init service,
        ;; so bring that up too, not just mssql.
        "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1434;Database=datum_test;Uid=sa;Pwd=DatumTest1!;TrustServerCertificate=yes")

       ("docker-mssql-admin"
        ;; Connects to master, for the same reason as docker-pg-admin.
        ;; This is also the one to use for the SQL Agent jobs panel.
        "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1434;Database=master;Uid=sa;Pwd=DatumTest1!;TrustServerCertificate=yes")

       ("docker-mysql"
        "Driver={MariaDB};Server=127.0.0.1;Port=3307;Database=datum_test;Uid=root;Pwd=datum_test")

       ("docker-oracle"
        "Driver={Oracle 23 ODBC driver};DBQ=127.0.0.1:1522/FREEPDB1;Uid=datum_test;Pwd=datum_test")))
  (let ((name (car entry))
        (conn-string (cadr entry)))
    (setq sql-connection-alist
          (cons `(,name
                  (sql-product 'datum)
                  (sql-send-terminator ";;")
                  (sql-server "")
                  (sql-database "")
                  (sql-user "")
                  (sql-password "")
                  (sql-datum-options (list ,(concat "--conn-string=" conn-string))))
                (assoc-delete-all name sql-connection-alist)))))

(provide 'docker-connections)
;;; docker-connections.el ends here
