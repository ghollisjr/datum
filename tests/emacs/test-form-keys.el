;;; test-form-keys.el --- Batch tests for wizard form navigation  -*- lexical-binding: t; -*-

;; Keyboard navigation in the admin wizard forms.
;;
;; The <tab> cases matter because a graphical Emacs sends the <tab>
;; function key and only falls back to the ASCII TAB binding when <tab>
;; is unbound everywhere.  A config that binds <tab> globally — to a
;; completion command, say — otherwise shadows widget navigation through
;; the whole form, so these tests install exactly such a binding first.
;;
;; Run with:
;;   emacs --batch -l sql-datum.el -l test-form-keys.el

;;; Code:

(require 'cl-lib)

(defvar test-form-keys--pass 0)
(defvar test-form-keys--fail 0)
(defvar test-form-keys--sent nil)

(defmacro test-form-keys-assert (description test-form)
  "Assert TEST-FORM is non-nil; report DESCRIPTION."
  `(condition-case err
       (if ,test-form
           (progn
             (setq test-form-keys--pass (1+ test-form-keys--pass))
             (message "  PASS: %s" ,description))
         (setq test-form-keys--fail (1+ test-form-keys--fail))
         (message "  FAIL: %s" ,description))
     (error
      (setq test-form-keys--fail (1+ test-form-keys--fail))
      (message "  FAIL: %s (error: %s)" ,description err))))

(advice-add 'sql-datum--admin-send-command-to :override
            (lambda (_buf cmd) (setq test-form-keys--sent cmd)))

;; Stand in for a global completion binding on <tab>.
(defun test-form-keys--hostile-tab ()
  (interactive)
  (error "company not available"))
(global-set-key (kbd "<tab>") #'test-form-keys--hostile-tab)

(defun test-form-keys--form (fields)
  "Render a form made of FIELDS (a JSON array string)."
  (sql-datum--admin-show-form
   (sql-datum--admin-denull-alist
    (json-parse-string
     (concat "{\"panel\":\"databases\",\"form\":{\"fields\":" fields
             ",\"values\":{},\"submit_action\":\"create-database\","
             "\"submit_label\":\"Create\",\"notes\":[]}}")
     :object-type 'alist :array-type 'list))
   nil)
  (get-buffer "*datum-admin:databases-form*"))

(defconst test-form-keys--two-paths
  (concat "[{\"key\":\"name\",\"label\":\"Database Name\","
          "\"type\":\"string\",\"default\":\"\"},"
          "{\"key\":\"data_dir\",\"label\":\"Data File Directory\","
          "\"type\":\"path\",\"default\":\"/var/opt/mssql/data\"},"
          "{\"key\":\"log_dir\",\"label\":\"Log File Directory\","
          "\"type\":\"path\",\"default\":\"/var/opt/mssql/log\"}]"))

(defconst test-form-keys--one-path
  (concat "[{\"key\":\"name\",\"label\":\"Database Name\","
          "\"type\":\"string\",\"default\":\"\"},"
          "{\"key\":\"data_dir\",\"label\":\"Data File Directory\","
          "\"type\":\"path\",\"default\":\"/var/opt/mssql/data\"}]"))

(defconst test-form-keys--no-paths
  (concat "[{\"key\":\"name\",\"label\":\"Database Name\","
          "\"type\":\"string\",\"default\":\"\"}]"))

(defun test-form-keys--browsed-field ()
  "Return the field key in the last captured browse command."
  (let ((b64 (car (last (split-string test-form-keys--sent " ")))))
    (alist-get 'field (json-parse-string (base64-decode-string b64)
                                         :object-type 'alist :array-type 'list))))

(message "\n=== Wizard form keys ===")

;; --- <tab> must win over a hostile global binding everywhere ---
(with-current-buffer (test-form-keys--form test-form-keys--two-paths)
  (goto-char (point-min))
  (widget-forward 1)
  (test-form-keys-assert "<tab> reaches the form inside a field"
                         (eq (key-binding (kbd "<tab>"))
                             'sql-datum-form-tab))
  (test-form-keys-assert "TAB reaches it too inside a field"
                         (eq (key-binding (kbd "TAB"))
                             'sql-datum-form-tab))
  (goto-char (point-min))
  (test-form-keys-assert "<tab> reaches the form on protected text"
                         (eq (key-binding (kbd "<tab>"))
                             'sql-datum-form-tab))
  (test-form-keys-assert "<backtab> goes back"
                         (eq (key-binding (kbd "<backtab>")) 'widget-backward))
  (test-form-keys-assert "S-<tab> goes back"
                         (eq (key-binding (kbd "S-<tab>")) 'widget-backward))
  (test-form-keys-assert "<up>/<down> move between fields"
                         (and (eq (key-binding (kbd "<down>"))
                                  'sql-datum-form-next-field)
                              (eq (key-binding (kbd "<up>"))
                                  'sql-datum-form-prev-field)))
  (test-form-keys-assert "C-c C-f is bound to browse"
                         (eq (key-binding (kbd "C-c C-f"))
                             'sql-datum-form-browse))

  ;; --- reaching Browse without walking to the button ---
  (test-form-keys-assert "both path fields are found"
                         (equal (sort (mapcar #'car
                                              (sql-datum--form-path-fields))
                                      #'string<)
                                '("data_dir" "log_dir")))
  (goto-char (point-min))
  (search-forward "/var/opt/mssql/log")
  (sql-datum-form-browse)
  (test-form-keys-assert "C-c C-f browses the path field at point"
                         (equal (test-form-keys--browsed-field) "log_dir")))

;; --- a single path field needs no prompt, wherever point is ---
(with-current-buffer (test-form-keys--form test-form-keys--one-path)
  (goto-char (point-min))
  (sql-datum-form-browse)
  (test-form-keys-assert "C-c C-f uses the only path field from anywhere"
                         (equal (test-form-keys--browsed-field) "data_dir")))

;; --- forms with no path fields refuse rather than prompt ---
(with-current-buffer (test-form-keys--form test-form-keys--no-paths)
  (test-form-keys-assert "C-c C-f refuses when there is nothing to browse"
                         (condition-case nil
                             (progn (sql-datum-form-browse) nil)
                           (error t)))
  (test-form-keys-assert "the header omits browse when unavailable"
                         (not (string-match-p
                               "C-c C-f"
                               (substring-no-properties header-line-format)))))

(with-current-buffer (test-form-keys--form test-form-keys--one-path)
  ;; The keys live in the header line, which stays visible however far
  ;; a long form scrolls.
  (test-form-keys-assert "the header advertises browse when available"
                         (string-match-p
                          "C-c C-f"
                          (substring-no-properties header-line-format)))
  (test-form-keys-assert "static text is still read-only"
                         (condition-case nil
                             (progn (goto-char (point-min))
                                    (insert "X") nil)
                           (error t))))

;; --- the browser is navigable under the same hostile binding ---
(sql-datum--admin-show-path-browser
 (sql-datum--admin-denull-alist
  (json-parse-string
   (concat "{\"panel\":\"databases\",\"sub_panel\":\"path-browser\","
           "\"title\":\"Browse: /tmp\",\"headers\":[\"Dir\",\"Name\",\"Path\"],"
           "\"rows\":[[\"/\",\"..\",\"/\"],[\"/\",\"sub\",\"/tmp/sub\"]],"
           "\"row_id\":2,\"actions\":[],\"info\":null,"
           "\"context\":{\"path\":\"/tmp\",\"field\":\"data_dir\","
           "\"values\":{},\"return_action\":\"new-database\"}}")
   :object-type 'alist :array-type 'list))
 nil)
(with-current-buffer "*datum-admin:path-browser*"
  (test-form-keys-assert "<tab> moves in the path browser"
                         (eq (key-binding (kbd "<tab>")) 'next-line))
  (test-form-keys-assert "<down> moves in the path browser"
                         (eq (key-binding (kbd "<down>")) 'next-line)))


;; --- a repeating list stays in column whatever it holds ---
;;
;; `editable-field' treats :size as a minimum, so a value longer than it
;; grows the field and pushes every field after it out of column.  The
;; widths are therefore taken from the widest value actually present.

(defun test-form-keys--list-form (rows)
  "Render a column list whose rows are ROWS (a JSON array)."
  (sql-datum--admin-show-form
   (sql-datum--admin-denull-alist
    (json-parse-string
     (concat "{\"panel\":\"schema\",\"form\":{\"fields\":["
             "{\"key\":\"columns\",\"label\":\"Columns\","
             "\"type\":\"list\",\"default\":" rows ","
             "\"item\":["
             "{\"key\":\"name\",\"label\":\"Name\","
             "\"type\":\"string\",\"size\":18},"
             "{\"key\":\"type\",\"label\":\"Type\","
             "\"type\":\"choice\",\"choices\":"
             "[[\"BIT\",\"BIT\"],"
             "[\"INT IDENTITY(1,1)\",\"INT IDENTITY(1,1)\"]]},"
             "{\"key\":\"nullable\",\"label\":\"Null\","
             "\"type\":\"bool\"},"
             "{\"key\":\"pk\",\"label\":\"PK\",\"type\":\"bool\"}]}],"
             "\"values\":{},\"submit_action\":\"create-table\","
             "\"submit_label\":\"Create\",\"notes\":[]}}")
     :object-type 'alist :array-type 'list))
   nil)
  (get-buffer "*datum-admin:schema-form*"))

(defun test-form-keys--completing-form ()
  "Render a column list whose type column completes as you type."
  (sql-datum--admin-show-form
   (sql-datum--admin-denull-alist
    (json-parse-string
     (concat "{\"panel\":\"schema\",\"form\":{\"fields\":["
             "{\"key\":\"columns\",\"label\":\"Columns\","
             "\"type\":\"list\","
             "\"default\":[[\"id\",\"BIT\",false,true]],"
             "\"item\":["
             "{\"key\":\"name\",\"label\":\"Name\","
             "\"type\":\"string\",\"size\":18},"
             "{\"key\":\"type\",\"label\":\"Type\","
             "\"type\":\"completing\",\"size\":22,"
             "\"completions\":[\"BIT\",\"INT\","
             "\"INT IDENTITY(1,1)\","
             "\"NVARCHAR(50)\",\"NVARCHAR(MAX)\"]},"
             "{\"key\":\"nullable\",\"label\":\"Null\","
             "\"type\":\"bool\"},"
             "{\"key\":\"pk\",\"label\":\"PK\",\"type\":\"bool\"}]}],"
             "\"values\":{},\"submit_action\":\"create-table\","
             "\"submit_label\":\"Create\",\"notes\":[]}}")
     :object-type 'alist :array-type 'list))
   nil)
  (get-buffer "*datum-admin:schema-form*"))

(defun test-form-keys--checkbox-columns ()
  "Return the column each row's first checkbox starts at."
  (let (offsets)
    (goto-char (point-min))
    (while (not (eobp))
      (let ((line (buffer-substring-no-properties
                   (line-beginning-position) (line-end-position))))
        (when (string-match-p "\\[INS\\] \\[DEL\\]" line)
          (push (string-match "\\[X\\]\\|\\[ \\]" line) offsets)))
      (forward-line 1))
    (nreverse offsets)))

(with-current-buffer (test-form-keys--list-form
                      (concat "[[\"id\",\"INT IDENTITY(1,1)\",false,true],"
                              "[\"short\",\"BIT\",true,false]]"))
  (let ((columns (test-form-keys--checkbox-columns)))
    (test-form-keys-assert "rows of similar width line up"
                           (and columns (apply #'= columns)))))

(with-current-buffer (test-form-keys--list-form
                      (concat "[[\"id\",\"INT IDENTITY(1,1)\",false,true],"
                              "[\"a_very_long_column_name_here\",\"BIT\","
                              "true,false],"
                              "[\"short\",\"BIT\",true,false]]"))
  (let ((columns (test-form-keys--checkbox-columns)))
    (test-form-keys-assert "a wide name does not shift its row"
                           (and (= 3 (length columns))
                                (apply #'= columns))))
  (test-form-keys-assert "the legend still sits over the boxes"
                         (save-excursion
                           (goto-char (point-min))
                           (search-forward "Null" nil t)))
  (test-form-keys-assert "a heading is not clipped by a narrow column"
                         (save-excursion
                           (goto-char (point-min))
                           (and (search-forward "Null" nil t)
                                (search-forward "PK" nil t)))))


;; --- vertical movement keeps the column ---
;;
;; `widget-forward' walks the widgets in creation order, so from the
;; middle of a row it steps along that row rather than down the column,
;; sliding to the last field of the row above and stopping there.

(defun test-form-keys--row-lines ()
  "Return the line numbers of the list's rows."
  (let (lines)
    (goto-char (point-min))
    (while (not (eobp))
      (when (string-match-p "\\[INS\\] \\[DEL\\]"
                            (buffer-substring-no-properties
                             (line-beginning-position) (line-end-position)))
        (push (line-number-at-pos) lines))
      (forward-line 1))
    (nreverse lines)))

(with-current-buffer (test-form-keys--list-form
                      (concat "[[\"id\",\"INT IDENTITY(1,1)\",false,true],"
                              "[\"two\",\"BIT\",true,false],"
                              "[\"three\",\"BIT\",true,false]]"))
  (let* ((rows (test-form-keys--row-lines))
         (last-row (car (last rows))))
    (test-form-keys-assert "the list rendered three rows" (= 3 (length rows)))
    ;; Walking up from each kind of column must stay in that column.
    (dolist (probe '(("a name column" . 12)
                     ("a type column" . 32)
                     ("a checkbox column" . 55)))
      (goto-char (point-min))
      (forward-line (1- last-row))
      (move-to-column (cdr probe))
      (let ((column (current-column))
            (kept t))
        (dotimes (_ 2)
          (sql-datum-form-prev-field)
          (unless (= (current-column) column) (setq kept nil)))
        (test-form-keys-assert
         (format "moving up keeps %s" (car probe)) kept)))
    ;; And the same going down.
    (goto-char (point-min))
    (forward-line (1- (car rows)))
    (move-to-column 55)
    (let ((column (current-column)) (kept t))
      (dotimes (_ 2)
        (sql-datum-form-next-field)
        (unless (= (current-column) column) (setq kept nil)))
      (test-form-keys-assert "moving down keeps the column" kept))))

;; An ordinary one-field-per-line form still walks field to field.
(with-current-buffer (test-form-keys--form test-form-keys--two-paths)
  (goto-char (point-min))
  (widget-forward 1)
  (let ((first (point)))
    (sql-datum-form-next-field)
    (test-form-keys-assert "a single-column form still moves between fields"
                           (and (/= (point) first)
                                (sql-datum--form-widget-at-point)))
    (sql-datum-form-prev-field)
    (test-form-keys-assert "and comes back"
                           (= (point) first))))

;; --- long rows must not wrap ---
(with-current-buffer (test-form-keys--form test-form-keys--two-paths)
  (test-form-keys-assert "a form buffer does not wrap"
                         (eq truncate-lines t)))


;; --- the keys must work inside a list's own sub-widgets too ---
;;
;; A widget built inside the list group carries wid-edit's keymap unless
;; it is given the form's, and a global <tab> binding then shadows field
;; movement there — while still working everywhere else in the form,
;; which is what made it easy to miss.

(with-current-buffer (test-form-keys--list-form
                      (concat "[[\"id\",\"INT IDENTITY(1,1)\",false,true],"
                              "[\"two\",\"BIT\",true,false]]"))
  (let ((row (progn (goto-char (point-min))
                    (while (and (not (eobp))
                                (not (string-match-p
                                      "\\[INS\\] \\[DEL\\]"
                                      (buffer-substring-no-properties
                                       (line-beginning-position)
                                       (line-end-position)))))
                      (forward-line 1))
                    (line-number-at-pos))))
    (dolist (probe '(("a name text box" . 12)
                     ("a type menu" . 32)))
      (goto-char (point-min))
      (forward-line (1- row))
      (move-to-column (cdr probe))
      (test-form-keys-assert
       (format "<tab> reaches the form from %s" (car probe))
       (eq (key-binding (kbd "<tab>")) 'sql-datum-form-tab))
      (test-form-keys-assert
       (format "<backtab> goes back in %s" (car probe))
       (eq (key-binding (kbd "<backtab>")) 'widget-backward))
      (test-form-keys-assert
       (format "C-c C-c submits from %s" (car probe))
       (eq (key-binding (kbd "C-c C-c")) 'sql-datum-form-submit)))))


;; --- a payload too long for one line is chunked ---
;;
;; A pty truncates input at 4095 bytes silently, so a long command
;; arrives cut in half and its base64 fails to decode.

(let ((sent nil))
  (cl-letf (((symbol-function 'sql-datum--admin-send-command-to)
             (lambda (_buf cmd) (push cmd sent)))
            ((symbol-function 'sql-datum--admin-send-commands-to)
             (lambda (_buf cmds) (setq sent (reverse cmds)))))
    (setq sent nil)
    (sql-datum--admin-send-with-payload nil ":admin-action p a" "SHORT")
    (test-form-keys-assert "a short payload goes in one command"
                           (equal (nreverse sent)
                                  '(":admin-action p a SHORT")))

    (setq sent nil)
    (sql-datum--admin-send-with-payload
     nil ":admin-action p a" (make-string 9000 ?A))
    (let ((cmds (reverse sent)))
      (test-form-keys-assert "a long payload is split"
                             (> (length cmds) 2))
      (test-form-keys-assert "every line stays under what a pty carries"
                             (cl-every (lambda (c)
                                         (< (length c) 4095))
                                       cmds))
      (test-form-keys-assert "the chunks come first"
                             (cl-every (lambda (c)
                                         (string-prefix-p ":admin-payload" c))
                                       (butlast cmds)))
      (test-form-keys-assert "the action names them last"
                             (equal (car (last cmds))
                                    ":admin-action p a @payload"))
      (test-form-keys-assert "no payload is lost in the split"
                             (equal (make-string 9000 ?A)
                                    (mapconcat
                                     (lambda (c)
                                       (substring c (length ":admin-payload ")))
                                     (butlast cmds) ""))))))


;; --- a menu must still open ---
;;
;; The form's keymap comes in two flavours: the field one binds RET to
;; `widget-field-activate', which is right for typing into a text box
;; and does nothing at all on a menu.  Giving a menu that keymap leaves
;; its value impossible to change — every new column stuck on whatever
;; type it defaulted to.

(with-current-buffer (test-form-keys--list-form
                      "[[\"id\",\"INT IDENTITY(1,1)\",false,true]]")
  (let ((menu nil) (field nil))
    (goto-char (point-min))
    (while (not (eobp))
      (let ((w (or (widget-field-at (point)) (widget-at (point)))))
        (cond ((and (null menu) w (eq (widget-type w) 'menu-choice))
               (setq menu (point)))
              ((and (null field) w (eq (widget-type w) 'editable-field))
               (setq field (point)))))
      (forward-char 1))
    (test-form-keys-assert "a row has a menu and a text box" (and menu field))
    (goto-char menu)
    (test-form-keys-assert "RET opens a menu in a list row"
                           (eq (key-binding (kbd "RET"))
                               'widget-button-press))
    (test-form-keys-assert "<tab> still reaches the form from a menu"
                           (eq (key-binding (kbd "<tab>"))
                               'sql-datum-form-tab))
    (goto-char field)
    (test-form-keys-assert "RET still edits in a text box"
                           (eq (key-binding (kbd "RET"))
                               'widget-field-activate))))

;; And a menu's value can actually be set, including on an inserted row.
(with-current-buffer (test-form-keys--list-form
                      "[[\"id\",\"INT IDENTITY(1,1)\",false,true]]")
  (let ((lw (cddr (assoc "columns" sql-datum--form-widgets))))
    (goto-char (point-min))
    (search-forward "[INS]")
    (backward-char 2)
    (widget-button-press (point))
    (test-form-keys-assert "INS adds a row"
                           (= 2 (length (widget-value lw))))
    (let ((rows (copy-tree (widget-value lw))))
      (setf (nth 1 (nth 0 rows)) "BIT")
      (widget-value-set lw rows)
      (widget-setup))
    (test-form-keys-assert "an inserted row's type can be changed"
                           (equal (nth 1 (nth 0 (widget-value lw))) "BIT"))))


;; --- TAB completes where there is something to complete ---
;;
;; M-TAB is the widget library's completion key, but a window manager
;; commonly takes it before Emacs sees it, so TAB does both jobs.

(with-current-buffer (test-form-keys--completing-form)
  (let ((lw (cddr (assoc "columns" sql-datum--form-widgets))))
    (cl-flet* ((type-field ()
                 ;; Found afresh each time: completing a value rewrites
                 ;; the buffer, so a remembered position goes stale.
                 (goto-char (point-min))
                 (let ((found nil))
                   (while (and (not found) (not (eobp)))
                     (let ((f (widget-field-at (point))))
                       (when (and f (widget-get f :completions))
                         (setq found (point))))
                     (unless found (forward-char 1)))
                   (goto-char found)
                   (widget-field-at (point))))
               (set-type (v)
                 (let ((f (type-field)))
                   (delete-region (widget-field-start f)
                                  (widget-field-end f))
                   (goto-char (widget-field-start f))
                   (insert v))))
      (test-form-keys-assert "TAB is the complete-or-move command"
                             (progn (type-field)
                                    (eq (key-binding (kbd "TAB"))
                                        'sql-datum-form-tab)))
      ;; Lower case must still find the upper-case candidates.
      (set-type "In")
      (test-form-keys-assert "a lower-case prefix still matches"
                             (equal (sql-datum--form-completion-at-point)
                                    '(expand . "INT")))
      (sql-datum-form-tab)
      (test-form-keys-assert "TAB folds case when completing"
                             (equal (nth 1 (nth 0 (widget-value lw))) "INT"))

      ;; A value typed in the wrong case is rewritten, not left as-is.
      (set-type "int")
      (sql-datum-form-tab)
      (test-form-keys-assert "TAB rewrites a mis-cased value"
                             (equal (nth 1 (nth 0 (widget-value lw))) "INT"))

      ;; INT is a candidate outright, and also the start of
      ;; INT IDENTITY(1,1) -- it must not trap point in the cell.
      (set-type "INT")
      (test-form-keys-assert "a candidate that prefixes another is done"
                             (null (sql-datum--form-completion-at-point)))
      (let ((before (point)))
        (sql-datum-form-tab)
        (test-form-keys-assert "so TAB leaves rather than sticking"
                               (/= (point) before)))

      ;; A common prefix that is not yet a type -- NVARCHAR( still
      ;; needs a size -- offers the candidates rather than moving on
      ;; and leaving something that will not compile.
      (set-type "NV")
      (test-form-keys-assert "a shared prefix expands first"
                             (equal (sql-datum--form-completion-at-point)
                                    '(expand . "NVARCHAR(")))
      (set-type "NVARCHAR(")
      (test-form-keys-assert "then offers the sizes it could be"
                             (equal (car (sql-datum--form-completion-at-point))
                                    'show))

      (set-type "INT IDENT")
      (test-form-keys-assert "a partial value has something to complete"
                             (sql-datum--form-completion-at-point))
      (sql-datum-form-tab)
      (test-form-keys-assert "TAB completes it"
                             (equal (nth 1 (nth 0 (widget-value lw)))
                                    "INT IDENTITY(1,1)"))

      (set-type "BIT")
      (test-form-keys-assert "an exact value has nothing to complete"
                             (null (sql-datum--form-completion-at-point)))
      (let ((before (point)))
        (sql-datum-form-tab)
        (test-form-keys-assert "so TAB moves on instead"
                               (/= (point) before)))

      ;; A size the suggestions do not name must be left alone rather
      ;; than fought with.
      (set-type "NVARCHAR(120)")
      (test-form-keys-assert "an unlisted size has nothing to complete"
                             (null (sql-datum--form-completion-at-point)))
      (sql-datum-form-tab)
      (test-form-keys-assert "and TAB leaves it as typed"
                             (equal (nth 1 (nth 0 (widget-value lw)))
                                    "NVARCHAR(120)"))

      (set-type "")
      (test-form-keys-assert "an empty field just moves on"
                             (null (sql-datum--form-completion-at-point))))))

;; A field with no candidates at all always moves.
(with-current-buffer (test-form-keys--form test-form-keys--one-path)
  (goto-char (point-min))
  (widget-forward 1)
  (test-form-keys-assert "a field without candidates has nothing to complete"
                         (null (sql-datum--form-completion-at-point)))
  (let ((before (point)))
    (sql-datum-form-tab)
    (test-form-keys-assert "so TAB moves between fields as before"
                           (/= (point) before))))


;; --- SPC toggles a checkbox, RET still does too ---
;;
;; SPC must keep typing a space inside a text field, so it is bound on
;; the buffer's map only -- fields carry a keymap of their own.

(with-current-buffer (test-form-keys--completing-form)
  (let ((lw (cddr (assoc "columns" sql-datum--form-widgets)))
        (box (progn (goto-char (point-min))
                    (let (found)
                      (while (and (not found) (not (eobp)))
                        (let ((w (widget-at (point))))
                          (when (and w (eq (widget-type w) 'checkbox))
                            (setq found (point))))
                        (unless found (forward-char 1)))
                      found))))
    (test-form-keys-assert "a form has a checkbox to toggle" box)
    (goto-char box)
    (test-form-keys-assert "SPC toggles at a checkbox"
                           (eq (key-binding (kbd "SPC"))
                               'sql-datum-form-toggle))
    (let ((before (nth 2 (nth 0 (widget-value lw)))))
      (call-interactively #'sql-datum-form-toggle)
      (test-form-keys-assert "SPC flips the box"
                             (not (eq before
                                      (nth 2 (nth 0 (widget-value lw))))))
      (goto-char box)
      (call-interactively #'sql-datum-form-toggle)
      (test-form-keys-assert "and flips it back"
                             (eq before
                                 (nth 2 (nth 0 (widget-value lw)))))
      (goto-char box)
      (widget-button-press (point))
      (test-form-keys-assert "RET still toggles as before"
                             (not (eq before
                                      (nth 2 (nth 0 (widget-value lw)))))))
    ;; The whole point of binding SPC narrowly.
    (goto-char (point-min))
    (let (fld)
      (while (and (not fld) (not (eobp)))
        (let ((f (widget-field-at (point))))
          (when (and f (widget-get f :completions)) (setq fld (point))))
        (unless fld (forward-char 1)))
      (goto-char fld)
      (test-form-keys-assert "SPC still self-inserts in a text field"
                             (eq (key-binding (kbd "SPC"))
                                 'self-insert-command))
      (let ((f (widget-field-at (point))))
        (delete-region (widget-field-start f) (widget-field-end f))
        (goto-char (widget-field-start f)))
      (dolist (ch (string-to-list "INT IDENTITY(1,1)"))
        (let ((last-command-event ch))
          (call-interactively #'self-insert-command)))
      (test-form-keys-assert "so a type with a space can be typed"
                             (equal (widget-apply (widget-field-at (point))
                                                  :value-get)
                                    "INT IDENTITY(1,1)")))))

;; The editors that predate the generic renderer answer to SPC too.
(test-form-keys-assert "the older widget editors toggle with SPC"
                       (with-temp-buffer
                         (use-local-map (sql-datum--widget-keymap))
                         (and (eq (key-binding (kbd "SPC"))
                                  'sql-datum-form-toggle)
                              (eq (key-binding (kbd "RET"))
                                  'widget-button-press))))

;; Off a checkbox SPC keeps its usual meaning.
(with-current-buffer (test-form-keys--form test-form-keys--one-path)
  (goto-char (point-min))
  (test-form-keys-assert "SPC on a label falls through to self-insert"
                         (eq (key-binding (kbd "SPC")) 'sql-datum-form-toggle))
  ;; Whatever self-insert then makes of it, the label is read-only and
  ;; must come through untouched.
  (let ((before (buffer-string)))
    (ignore-errors (call-interactively #'sql-datum-form-toggle))
    (test-form-keys-assert "and leaves the read-only label alone"
                           (equal before (buffer-string)))))

(message "\n%d passed, %d failed" test-form-keys--pass test-form-keys--fail)
(when (> test-form-keys--fail 0)
  (kill-emacs 1))

(provide 'test-form-keys)
;;; test-form-keys.el ends here
