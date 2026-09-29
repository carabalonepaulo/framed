package framed_ffi

import "base:runtime"
import "core:nbio"
import "core:net"

@(export)
framed_init :: proc "c" () {
	context = runtime.default_context()
	nbio.acquire_thread_event_loop()
}

@(export)
framed_deinit :: proc "c" () {
	context = runtime.default_context()
	nbio.release_thread_event_loop()
}

@(export)
framed_tick :: proc "c" () {
	context = runtime.default_context()
	nbio.tick(0)
}

Endpoint :: struct {
	addr: [4]u8,
	port: u16,
	ok:   bool,
}

@(export)
framed_parse_endpoint :: proc "c" (addr: [^]u8, addr_len: int) -> Endpoint {
	context = runtime.default_context()
	ep, ok := net.parse_endpoint(transmute(string)(addr[:addr_len]))
	addr, addr_ok := ep.address.(net.IP4_Address)
	return {addr = ([4]u8)(addr), port = u16(ep.port), ok = ok && addr_ok}
}

