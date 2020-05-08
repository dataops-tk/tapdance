# Authoring a rules file

_A rules file is like an `include` file that governs ths inclusion of tables and columns._

Notes and guidelines:

- Tables must be included as a two-part reference: tap_name.table_name
- The text `tap.MyTable.*` is shorthand for entering `tap.MyTable` and
  `tap.MyTable.*` on separate lines.
- Every line in this file represents a separate entry. Rules are evaluated in
  order from top to bottom with latter rules overriding earlier ones.
- All references are case insensitive (`**.name` will also match `Name`).
- Columns can be included by referencing: `tap_name.table_name.column_name`.
- Double wildcards (`**.`) can be used as shorthand for `*.*.`
- Comments are supported inline and are noted by the hash symbol (`#`).
- Rules can be negated with the `!` operator, such as to exclude all
  references to first name: `!**.first_name`
- Wildcards (*) are permitted anywhere but only operate within the scope
  between periods.
- Regular Expressions, marked by enveloping slashes (`/`), can be used,
  but only in column names, for example: `!**./credit.*card.*/`

Planned for V2:
- Columns prefixed with a tilde '~' will be obfuscated
  using a one-way hash, which defaults to MD5 with project name as the seed.
