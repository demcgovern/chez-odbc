;; Provides Scheme functions for ODBC.
;; @author Donlon Eamonn McGovern
(library (odbc)
  (export allocate-handle
          disconnect
          driver-connect
          execute-direct
          free-handle
          get-diagnostic-record
          get-diagnostic-records
          null-handle
          number-result-columns
          set-environment-attribute)

  (import (scheme))

;;; Constants
  
  (define null-handle 0)
  (define null-terminated-string -3)

;;; Syntax

  ;; Wraps an ODBC foreign procedure.
  ;; Translates the return value into a symbol.
  ;; Raises an error when appropriate.
  ;; @param name string
  ;; The name of the foreign procedure.
  ;; @param params list
  ;; An association list of the parameters and foreign types of the procedure.
  ;; @returns procedure
  ;; The wrapped foreign procedure.
  ;; @exception assertion-violation
  ;; The foreign procedure returned an unknown return value.
  ;; @exception error
  ;; The foreign procedure returned invalid handle, error, still executing, or no data.
  (define-syntax odbc-procedure
    (syntax-rules ()
      ((odbc-procedure name ((param param-type) ...))
       (let ((f (foreign-procedure name (param-type ...) short)))
         (lambda (param ...)
           (let* ((y (f param ...))
                  (y (case y
                       ((-2) 'invalid-handle)
                       ((-1) 'error)
                       ((0) 'success)
                       ((1) 'success-with-info)
                       ((2) 'still-executing)
                       ((100) 'no-data)
                       (else (assertion-violation name "unknown return value" y)))))
             (case y
               ((invalid-handle error still-executing no-data) (error name "foreign error" y))
               (else y))))))))

;;; Internal helper functions

  ;; Translates a handle type symbol into an integer.
  ;; @param handle-type symbol
  ;; @returns integer
  ;; @exception assertion-violation
  ;; The handle type is unknown.
  (define (handle-type->integer handle-type)
    (case handle-type
      ((environment) 1)
      ((connection) 2)
      ((statement) 3)
      ((descriptor) 4)
      (else (assertion-violation 'handle-type->integer "unknown handle type" handle-type))))

;;; ODBC functions

  ;; Wraps SQLAllocHandle.
  ;; @param handle-type symbol
  ;; @param input-handle integer
  ;; @returns integer
  (define allocate-handle
    (let ((f (odbc-procedure "SQLAllocHandle" ((handle-type short)
                                               (input-handle uptr)
                                               (output-handle-ptr (* uptr))))))
      (lambda (handle-type input-handle)
        (let ((output-handle-ptr (make-ftype-pointer uptr (foreign-alloc (foreign-sizeof 'uptr)))))
          (f (handle-type->integer handle-type) input-handle output-handle-ptr)
          (ftype-ref uptr () output-handle-ptr)))))

  ;; Wraps SQLDisconnect.
  ;; @param connection-handle integer
  ;; @returns symbol
  (define disconnect
    (let ((f (odbc-procedure "SQLDisconnect" ((connection-handle uptr)))))
      (lambda (connection-handle)
        (f connection-handle))))

  ;; Wraps SQLDriverConnect.
  ;; @param connection-handle integer
  ;; @param connection-string string
  ;; @returns symbol
  (define driver-connect
    (let ((f (odbc-procedure "SQLDriverConnectW" ((connection-handle uptr)
                                                  (window-handle uptr)
                                                  (in-connection-string wstring)
                                                  (string-length-1 short)
                                                  (out-connection-string uptr)
                                                  (buffer-length short)
                                                  (string-length-2-ptr (* short))
                                                  (driver-completion unsigned-short))))
          (buffer-length 0)
          (no-prompt 0))
      (lambda (connection-handle connection-string)
        (f connection-handle
           null-handle
           connection-string
           null-terminated-string
           null-handle
           buffer-length
           (make-ftype-pointer short (foreign-alloc (foreign-sizeof 'short)))
           no-prompt))))

  ;; Wraps SQLExecDirect.
  ;; @param statement-handle integer
  ;; @param statement-text string
  ;; @returns symbol
  (define execute-direct
    (let ((f (odbc-procedure "SQLExecDirectW" ((statement-handle uptr)
                                               (statement-text wstring)
                                               (text-length long)))))
      (lambda (statement-handle statement-text)
        (f statement-handle statement-text null-terminated-string))))

  ;; Wraps SQLFreeHandle.
  ;; @param handle-type symbol
  ;; @param handle integer
  ;; @returns symbol
  (define free-handle
    (let ((f (odbc-procedure "SQLFreeHandle" ((handle-type short)
                                              (handle uptr)))))
      (lambda (handle-type handle)
        (f (handle-type->integer handle-type) handle))))

  ;; Wraps SQLGetDiagRec.
  ;; @param handle-type symbol
  ;; @param handle integer
  ;; @param rec-number integer
  ;; @returns list
  ;; An association list of:
  ;; - sql-state : string
  ;; - native-error : integer
  ;; - message-text : string
  (define get-diagnostic-record
    (let ((f (odbc-procedure "SQLGetDiagRecW" ((handle-type short)
                                               (handle uptr)
                                               (rec-number short)
                                               (sql-state u16*)
                                               (native-error-ptr (* long))
                                               (message-text u16*)
                                               (buffer-length short)
                                               (text-length-ptr (* short)))))
          (buffer-length 1024))
      (lambda (handle-type handle rec-number)
        (let ((sql-state (make-bytevector 12))
              (native-error-ptr (make-ftype-pointer long (foreign-alloc (foreign-sizeof 'long))))
              (message-text (make-bytevector (* buffer-length 2)))
              (text-length-ptr (make-ftype-pointer short (foreign-alloc (foreign-sizeof 'short)))))
          (f (handle-type->integer handle-type) handle rec-number
             sql-state
             native-error-ptr
             message-text buffer-length text-length-ptr)
          (let* ((sql-state (substring (utf16->string sql-state 'little) 0 5))
                 (native-error (ftype-ref long () native-error-ptr))
                 (text-length (ftype-ref short () text-length-ptr))
                 (message-text (substring (utf16->string message-text 'little) 0 text-length)))
            (list (list 'sql-state sql-state)
                  (list 'native-error native-error)
                  (list 'message-text message-text)))))))

  ;; Wraps SQLNumResultCols
  ;; @param statement-handle integer
  ;; @returns integer
  (define number-result-columns
    (let ((f (odbc-procedure "SQLNumResultCols" ((statement-handle uptr)
                                                 (column-count-ptr (* short))))))
      (lambda (statement-handle)
        (let ((column-count-ptr (make-ftype-pointer short (foreign-alloc (foreign-sizeof 'short)))))
          (f statement-handle column-count-ptr)
          (ftype-ref short () column-count-ptr)))))

  ;; Wraps SQLSetEnvAttr.
  ;; @param environment-handle integer
  ;; @param attribute symbol
  ;; @param value symbol
  ;; @returns symbol
  ;; @exception assertion-violation
  ;; The configuration (attribute value) is unknown.
  (define set-environment-attribute
    (let ((f (odbc-procedure "SQLSetEnvAttr" ((environment-handle uptr)
                                              (attribute long)
                                              (value-ptr uptr)
                                              (string-length long))))
          ;; Association list of configuration options.
          ;; ((attribute attribute-code ((value value-code) ...) ...)
          (configuration-options '((odbc-version 200 ((3.8 380)))
                                   (connection-pooling 201 ((off 0)
                                                            (one-per-driver 1)
                                                            (one-per-environment 2)
                                                            (driver-aware 3)))
                                   (connection-pool-match 202 ((strict 0)
                                                               (relaxed 1))))))
      (lambda (environment-handle attribute value)
        (let* ((attribute-entry (assoc attribute configuration-options))
               (value-entry (if attribute-entry (assoc value (caddr attribute-entry)) #f)))
          (if (not (and attribute-entry value-entry))
              (assertion-violation 'set-environment-attribute "unknown configuration" attribute value))
          (f environment-handle (cadr attribute-entry) (cadr value-entry) 0)))))

;;; External helper functions
  
  ;; Gets all diagnostic records for a handle.
  ;; @param handle-type symbol
  ;; @param handle integer
  ;; @returns list
  ;; A list of association lists.
  ;; @returns boolean
  ;; #f if there are no diagnostic records.
  ;; @see get-diagnostic-record
  (define (get-diagnostic-records handle-type handle)
    (letrec ((f (lambda (handle-type handle rec-number accumulator)
                  (let* ((diagnostic-record (get-diagnostic-record handle-type handle rec-number))
                         (rec-number (+ rec-number 1))
                         (accumulator (cons diagnostic-record accumulator)))
                    (guard (c ((error? c) (reverse accumulator)))
                      (f handle-type handle rec-number accumulator))))))
      (guard (c ((error? c) #f))
        (f handle-type handle 1 '())))))
