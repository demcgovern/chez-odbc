(load-shared-object "msodbcsql18.dll")

(import (prefix (odbc) sql:))

(define environment-handle
  (sql:allocate-handle 'environment sql:null-handle))

(sql:set-environment-attribute environment-handle 'odbc-version 3.8)
;; success

(define connection-handle
  (sql:allocate-handle 'connection environment-handle))

(define connection-string
  (string-append "driver={odbc driver 18 for sql server};"
                 "server=;"
                 "database=master;"
                 "uid=sa;"
                 "pwd=Password123;"
                 "encrypt=no;"))

(sql:driver-connect connection-handle connection-string)
;; success-with-info

(sql:get-diagnostic-records 'connection connection-handle)
;; (((sql-state "01000")
;;   (native-error 5701)
;;   (message-text "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Changed database context to 'master'."))
;;  ((sql-state "01000")
;;   (native-error 5703)
;;   (message-text "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Changed language setting to us_english.")))

(define statement-handle
  (sql:allocate-handle 'statement connection-handle))

(sql:execute-direct statement-handle "select 1")
;; success

(sql:number-result-columns statement-handle)
;; 1

(sql:free-handle 'statement statement-handle)
;; success

(sql:disconnect connection-handle)
;; success

(sql:free-handle 'connection connection-handle)
;; success

(sql:free-handle 'environment environment-handle)
;; success
