(load-shared-object "msodbcsql18.dll")

(import (prefix (odbc) sql:))

(define environment-handle
  (sql:allocate-handle 'environment sql:null-handle))

(sql:set-environment-attribute environment-handle 'odbc-version 3.8)

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

(sql:get-diagnostic-records 'connection connection-handle)

(sql:disconnect connection-handle)

(sql:free-handle 'connection connection-handle)

(sql:free-handle 'environment environment-handle)
