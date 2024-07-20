let
  pkgs = import <nixpkgs> {};
in pkgs.buildGoModule {
  name = "mercury";
  src = builtins.fetchGit ./.;

  vendorHash = "sha256-wJOAzeBCDJrSJWhhCrouSgAo+T8KQVNbOPcj0j2Kpu0=";
}
