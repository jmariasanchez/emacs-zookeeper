# Local dev environment helper (macOS with Nix toolchains lacking libiconv).
if [ "$(uname -s)" = Darwin ]; then
    export PATH=/usr/bin:$PATH CC=/usr/bin/clang CXX=/usr/bin/clang++
fi
