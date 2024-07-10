{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  name = "go development shell";
  packages = with pkgs; [
    go
    protobuf
    protoc-gen-go
    protoc-gen-go-grpc
  ];
}
