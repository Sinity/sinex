{ stdenv
, fetchurl
, dpkg
, lib
, version ? "0.3.3"
, sha256 ? "sha256-6VSbAZrrItYgnpKMhVqffC4fGp9zzPYaMB6/Bf+Ha/g="
}:

stdenv.mkDerivation rec {
  pname = "pg_jsonschema";
  inherit version;

  src = fetchurl {
    url = "https://github.com/supabase/pg_jsonschema/releases/download/v${version}/pg_jsonschema-v${version}-pg16-amd64-linux-gnu.deb";
    inherit sha256;
  };

  nativeBuildInputs = [ dpkg ];
  dontConfigure = true;
  dontBuild = true;
  dontStrip = true;
  dontFixup = true;

  unpackPhase = ''
    dpkg-deb -x $src .
  '';

  installPhase = ''
    mkdir -p $out/lib $out/share/postgresql/extension
    find . -name "*.so" -type f -exec cp {} $out/lib/ \;
    find . -name "*.sql" -type f -exec cp {} $out/share/postgresql/extension/ \;
    find . -name "*.control" -type f -exec cp {} $out/share/postgresql/extension/ \;
  '';

  meta = with lib; {
    description = "PostgreSQL JSON Schema validation extension";
    homepage = "https://github.com/supabase/pg_jsonschema";
    license = licenses.asl20;
    maintainers = with maintainers; [ ];
    platforms = platforms.linux;
  };
}
