# Authoring a rules file

_A rules file governs the inclusion of tables and columns for any given tap._

Notes and guidelines:

- Tables can be included by entering the table name alone on a new line.
- Columns can be included by referencing them after a table identifier,
  i.e. `table_name.column_name`.
- Wildcards (*) are permitted anywhere but only operate within the scope
  between periods.
- The rule `MyTable.*` is identical to entering `MyTable` and `MyTable.*` on
  separate lines.
- Rules in the file are evaluated in order from top to bottom, with latter rules
  overriding earlier ones.
- All references are case insensitive (`*.name` will match `Name`, `name`, and `nAME`).
- Comments are supported inline and are noted by the hash symbol (`#`).
- Rules can be negated with the `!` operator, such as to exclude all
  references to first name: `!*.first_name`
- Regular Expressions ("regex") can be used for more complicated rules, instead of simple
  "*" wildcards. Regex expressions must be bordered by enveloping slashes (`/`), and are
  only valid in column names. For example, the exclusion rule `!*./credit.*card.*/` will
  exclude `account.CreditCard`, `cust.credit_card`, and also `acct.credit_card_number`.
- If you get warnings about primary keys not being automatically detected, you can 
  denote primary key fields by appending the hint `-> primary-key` after the column 
  name. Ex: `account.id -> primary-key`

Planned for V2:
- Columns prefixed with a tilde '~' should be obfuscated
  using a one-way hash, which defaults to MD5 with project name as the seed.
