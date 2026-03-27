{ lib, pkgs }:

with lib;

let
  sanitizeName = lib.strings.sanitizeDerivationName;
in
{
  renderDatabaseUrl =
    db:
    "postgresql://${db.user}@${db.host}:${toString db.port}/${db.name}";

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
