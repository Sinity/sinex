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

  # Restart-trigger source paths for a set of candidate secret names, resolved
  # against `config.sinex.secrets.restartTriggers` (content-addressed SOURCE
  # paths -- the age-encrypted file or the declarative environment.etc source
  # -- never the stable decrypted runtime path under /run/agenix, which does
  # not change hash when rotated content is re-decrypted to the same place).
  # Only named/managed secrets get a trigger this way: an operator-supplied
  # explicit file path (outside the named registry) has no content-addressed
  # source Nix can hash at eval time, so it is intentionally excluded here --
  # callers that accept an explicit override are responsible for restarting
  # the consuming unit themselves when they rotate that path's content.
  restartTriggersForNames =
    triggerSources: names:
    unique (filter (p: p != null) (map (name: triggerSources.${name} or null) names));
}
