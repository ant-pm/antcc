{
    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
        flake-utils.url = "github:numtide/flake-utils";
    };
    outputs =
        {
            self,
            nixpkgs,
            flake-utils,
            ...
        }:
        flake-utils.lib.eachDefaultSystem (
            system:
            let
                pkgs = import nixpkgs { inherit system; };
            in
            {
devShells.default = pkgs.mkShell {
    shellHook = "export NIX_SHELL_NAME='cc-dist-dev'";
    nativeBuildInputs = with pkgs; [
        rustup
        gnumake
        pkg-config
        gcc          # explicit — wrapper shells out to gcc
        autoconf     # needed for curl's ./configure
        automake
        libtool
    ];
    buildInputs = with pkgs; [
        wget
        libarchive
        openssl
        curl.dev     # curl build deps (headers etc.) if you want --with-ssl
        zlib         # curl dep
    ];
};
            }
        );
}
