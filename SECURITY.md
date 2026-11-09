# Security policy

`sqlalchemy-adbc` opens connections on your behalf and handles credentials:
OAuth2 client secrets, bearer tokens, Kerberos/Negotiate tickets, Snowflake
tokens and a local config file that may name secrets through `env:`
references. A flaw in how it treats them matters more than most bugs, so
please report it privately.

## Reporting a vulnerability

Use GitHub's private reporting: open the
[Security tab](https://github.com/drls-io/sqlalchemy-adbc/security) and choose
**Report a vulnerability**. That creates a private advisory visible only to the
maintainers and you. Please do not open a public issue or pull request for it.

Include what you can of:

- the version (`pip show sqlalchemy-adbc`), SQLAlchemy and ADBC driver versions;
- the URL form or config profile involved, with secrets removed;
- the smallest steps that show the problem and what an attacker gains.

This is a small project run on a best-effort basis. We aim to acknowledge a
report within a week, agree a fix and disclosure date with you, and credit you
in the changelog unless you prefer not to be named.

## Supported versions

Fixes go into the latest release. Before 1.0 there are no backports; upgrade to
the newest version to receive a fix. Python 3.12 or newer is required, and an
older interpreter gets an old release from `pip` that will not have the fix
(see the README's Install section).

## What is in scope

Reports about this package's own handling of credentials and trust, for
example:

- a secret (client secret, token, password, ticket) reaching a log message, an
  exception message, a URL, or a place other than the intended endpoint;
- a URL or query string overriding what a config profile pins (token endpoint,
  SPN, client identity), or resolving `env:` references from outside the config
  file;
- the config file's trust checks (ownership and permission rules, never
  reading from the working directory) being bypassed;
- credentials minted for one connection being used for another, or tokens
  cached longer than their lifetime;
- SQL or identifier injection through values the dialect renders itself, such
  as DDL, quoting, type casts or Arrow helper statements.

## What is out of scope

- Vulnerabilities in the ADBC drivers, `pyarrow`, SQLAlchemy, `pyspnego` or the
  database or Flight SQL server. Report those to the project concerned; if this
  package makes one easier to reach, tell us as well.
- Issues that need an attacker who can already write your config file, set
  your environment variables or run code in your process.
- Using a trusted URL with credentials you chose to put in it. Prefer
  `auth_profile=` so secrets stay out of URLs, as the README describes.
- Findings from automated scanners without a demonstrated impact on this
  package.
