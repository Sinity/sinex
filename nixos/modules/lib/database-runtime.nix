{ lib, pkgs }:

with lib;

let
  sanitizeName = lib.strings.sanitizeDerivationName;

  # Percent-encode a single character if it is in the RFC-3986 reserved/unsafe set.
  # Only characters outside [A-Za-z0-9-._~!$&'()*+,;=] are encoded.
  # This covers the characters most likely to appear in pg credentials that would
  # break URL structure: @, :, /, ?, #, [, ], %, space, and common punctuation.
  encodeUrlChar = c:
    let
      safe = builtins.match "[A-Za-z0-9._~!$&'()*+,;=-]" c != null;
    in
    if safe then c
    else
      let
        table = {
          " " = "%20"; "\"" = "%22"; "#" = "%23"; "$" = "%24";
          "%" = "%25"; "&" = "%26"; "'" = "%27"; "(" = "%28"; ")" = "%29";
          "*" = "%2A"; "+" = "%2B"; "," = "%2C"; "/" = "%2F"; ":" = "%3A";
          ";" = "%3B"; "<" = "%3C"; "=" = "%3D"; ">" = "%3E"; "?" = "%3F";
          "@" = "%40"; "[" = "%5B"; "\\" = "%5C"; "]" = "%5D"; "^" = "%5E";
          "`" = "%60"; "{" = "%7B"; "|" = "%7C"; "}" = "%7D";
        };
      in
      table.${c} or c;

  # URL-encode a string for use as a PostgreSQL connection URL component
  # (username or database name — not the host or port).
  urlEncodeComponent = s:
    lib.concatStrings (map encodeUrlChar (lib.stringToCharacters s));
in
{
  renderDatabaseUrl =
    db:
    "postgresql://${urlEncodeComponent db.user}@${db.host}:${toString db.port}/${urlEncodeComponent db.name}";

  mkDatabasePasswordExec =
    {
      name,
      command,
      passwordFile ? null,
    }:
    if passwordFile == null then
      command
    else
      pkgs.writeShellScript "sinex-${sanitizeName name}-database-auth" ''
        set -euo pipefail

        password_file=${escapeShellArg (toString passwordFile)}
        if [ ! -r "$password_file" ]; then
          echo "[sinex] database password file $password_file is not readable" >&2
          exit 1
        fi

        export PGPASSWORD="$(tr -d '\r\n' < "$password_file")"
        exec ${command}
      '';

}
