{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    rust = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust,
    }:
    let
      lib = nixpkgs.lib;
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = lib.genAttrs supportedSystems;

      mkPkgs =
        system:
        import nixpkgs {
          inherit system;
          overlays = [
            rust.overlays.default
          ];
        };

      mkRustToolchain = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

      mkPm =
        pkgs:
        let
          rustToolchain = mkRustToolchain pkgs;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
        in
        rustPlatform.buildRustPackage {
          pname = "pm";
          version = "0.0.0";
          src = lib.cleanSource ./.;
          cargoLock.lockFile = ./Cargo.lock;

          nativeBuildInputs = [
            pkgs.pkg-config
          ];

          buildInputs = [ ];

          meta = {
            description = "process manager";
            mainProgram = "pm";
            platforms = supportedSystems;
          };
        };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = mkPkgs system;
          pm = mkPm pkgs;
        in
        {
          default = pm;
          pm = pm;
        }
      );

      apps = forAllSystems (
        system:
        let
          defaultApp = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/pm";
          };
        in
        {
          default = defaultApp;
          pm = defaultApp;
        }
      );

      overlays.default = lib.composeExtensions rust.overlays.default (final: prev: {
        pm = mkPm final;
      });

      devShells = forAllSystems (
        system:
        let
          pkgs = mkPkgs system;
          rustToolchain = mkRustToolchain pkgs;
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              pkgs.cargo-edit
              pkgs.wild
              pkgs.clang
              pkgs.pkg-config
              pkgs.ibm-plex
            ];
            env.RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
          };
        }
      );
    };
}
