{
  description = "Development environment for learning Spark Declarative Pipelines";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              python314
              jdk17
              uv
              just
              docker-client
              docker-compose
            ];

            JAVA_HOME = "${pkgs.jdk17}";
            # Use the Nix interpreter even when uv reads .python-version.
            UV_PYTHON = "${pkgs.python314}/bin/python3";
            UV_PYTHON_DOWNLOADS = "never";

            # Native Python wheels (e.g. grpcio/numpy) need these on NixOS.
            shellHook = pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isLinux ''
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ]}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            '';
          };
        });
    };
}
