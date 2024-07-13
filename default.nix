let
  pkgs = import <nixpkgs> {};
in pkgs.buildGoModule {
  name = "mercury";
  src = builtins.fetchGit ./.;

  vendorHash = "sha256-fJIy8TgtdxB9pA12dTnj96B6N/dkptRcpsHyF9sxWP4=";
}
