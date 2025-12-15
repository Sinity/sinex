final: prev:
let
  pgJsonschema = final.callPackage ../pkgs/pg_jsonschema { };

  timescaledbVersion = "2.23.0";
  timescaledbSrc = final.fetchFromGitHub {
    owner = "timescale";
    repo = "timescaledb";
    tag = timescaledbVersion;
    hash = "sha256-wgRaWxGr48p8xMc+yOQEN196KAKyptMCk/UFKn23cos=";
  };
in
{
  postgresql16Packages = prev.postgresql16Packages // {
    pg_jsonschema = pgJsonschema;
    timescaledb = prev.postgresql16Packages.timescaledb.overrideAttrs (_: {
      version = timescaledbVersion;
      src = timescaledbSrc;
    });
  };
}
