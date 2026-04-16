{ lib }:

with lib;

{
  resolveNamedSecretPath =
    secretPaths: explicit: names:
    if explicit != null then
      explicit
    else
      let
        match = findFirst (name: builtins.hasAttr name secretPaths) null names;
      in
      if match == null then null else builtins.getAttr match secretPaths;
}
