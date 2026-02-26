# based on https://github.com/the-nix-way/dev-templates/
{
  description = "Let's see you do this, conda";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

  outputs = inputs: let
    javaVersion = 17;

    supportedSystems = [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];
    forEachSupportedSystem = f:
      inputs.nixpkgs.lib.genAttrs supportedSystems (
        system:
          f {
            pkgs = import inputs.nixpkgs {
              inherit system;
              overlays = [inputs.self.overlays.default];
            };
          }
      );
  in {
    overlays.default = final: prev: let
      jdk = prev."jdk${toString javaVersion}";
    in {
      sbt = prev.sbt.override {jre = jdk;};
      scala = prev.scala_3.override {jre = jdk;};
    };

    devShells = forEachSupportedSystem (
      {pkgs}: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            python313
            python313Packages.duckdb
            python313Packages.polars
            python313Packages.pandas
            python313Packages.pyarrow
            python313Packages.pyspark
            python313Packages.py4j
            python313Packages.requests
            python313Packages.tqdm
            hadoop
            duckdb

            scala_2_13
            sbt
            coursier
            metals
            scalafmt
          ];

          SPARK_LOCAL_IP = "127.0.0.1";
          POLARS_MAX_THREADS = "1";
        };
      }
    );
  };
}
