let
  pkgs = import <nixpkgs> {};
in pkgs.buildGoModule {
  name = "mercury";
  src = builtins.fetchGit ./.;

  vendorHash = "sha256-dBcEXhgpaF1dmzKJ2069n3Z0OcRpnYOnYgdze1DYHPU=";
}
