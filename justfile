run:
    @-odin run demo

dyn:
    @odin build framed_ffi -build-mode:dll -o:aggressive

obj:
    @odin build framed_ffi -no-entry-point -build-mode:obj -o:aggressive

static:
    @odin build framed_ffi -no-entry-point -build-mode:static -o:aggressive

framed_load_test:
    @-odin build load_test/framed -o:aggressive

enet_load_test:
    @-odin build load_test/enet -o:aggressive

telepathy_load_test:
    @cd load_test/telepathy && dotnet build -c Release
