let
  lib = import "${import ./dep/nixpkgs/thunk.nix}/lib/default.nix";
  packageImport = args:
    import ./dep/nixpkgs/default.nix args;

  makePackage = { pkgs }: pkgs.buildGoModule {
    name = "mercury";
    src = builtins.fetchGit ./.;

    vendorHash = "sha256-wJOAzeBCDJrSJWhhCrouSgAo+T8KQVNbOPcj0j2Kpu0=";

    meta = with lib; {
      description = "A Canton ODS implementation in Go";
      platforms = platforms.all;
      #license = license.mit;
    };
  };

in {
  linux = {
    amd64 = makePackage {
      pkgs = packageImport {
        system = "x86_64-linux";
      };
    };
    aarch64 = makePackage {
      pkgs = packageImport {
        system = "aarch64-linux";
      };
    };
  };

  macos = {
    amd64 = makePackage {
      pkgs = packageImport {
        system = "x86_64-darwin";
      };
    };
    aarch64 = makePackage {
      pkgs = packageImport {
        system = "aarch64-darwin";
      };
    };
  };

  windows = {
    amd64 = makePackage {
      pkgs = packageImport {
        crossSystem = lib.systems.examples.mingwW64;
      };
    };
  };
}
