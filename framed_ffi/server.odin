package framed_ffi

import "base:runtime"
import "core:net"

import "../framed/server"
import "../framed/shared"

Framed_Server_Callbacks :: struct {
	connected:       proc "c" (server: ^Framed_Server, id: int),
	disconnected:    proc "c" (server: ^Framed_Server, id: int),
	packet_received: proc "c" (server: ^Framed_Server, id: int, buf: [^]u8, buf_len: int),
	closed:          proc "c" (server: ^Framed_Server),
}

Framed_Server_Options :: struct {
	prefix_len:          int,
	endian:              shared.Endian,
	max_clients:         int,
	max_packet_size:     int,
	max_packet_backlog:  int,
	max_packet_per_tick: int,
	addr:                [4]u8,
	port:                u16,
	callbacks:           Framed_Server_Callbacks,
}

Framed_Server :: struct {
	inner:     server.Server,
	callbacks: Framed_Server_Callbacks,
}

@(export)
framed_server_create :: proc "c" (opts: Framed_Server_Options) -> ^Framed_Server {
	context = runtime.default_context()

	self, err := new(Framed_Server)
	if err != nil do return nil

	inner_opts := server.Options {
		prefix_len = opts.prefix_len,
		endian = opts.endian,
		max_clients = opts.max_clients,
		max_packet_size = opts.max_packet_size,
		max_packet_backlog = opts.max_packet_backlog,
		max_packets_per_tick = opts.max_packet_per_tick,
		endpoint = net.Endpoint{net.IP4_Address(opts.addr), int(opts.port)},
		callbacks = {
			client_connected = on_client_connected,
			client_disconnected = on_client_disconnected,
			packet_received = on_packet_received,
			closed = on_closed,
		},
	}

	if !server.init(&self.inner, inner_opts) {
		free(self)
		return nil
	}

	self.callbacks = opts.callbacks
	return self
}

@(export)
framed_server_destroy :: proc "c" (self: ^Framed_Server) {
	context = runtime.default_context()
	server.deinit(&self.inner)
	self^ = {}
	free(self)
}

@(export)
framed_server_close :: proc "c" (self: ^Framed_Server) {
	context = runtime.default_context()
	server.close(&self.inner)
}

@(export)
framed_server_is_closed :: proc "c" (self: ^Framed_Server) -> bool {
	context = runtime.default_context()
	return server.is_closed(&self.inner)
}

@(export)
framed_server_set_ud :: proc "c" (self: ^Framed_Server, idx: int, ud: rawptr) {
	if idx < 0 || idx >= len(self.inner.ud) do return
	#no_bounds_check {
		self.inner.ud[idx] = ud
	}
}

@(export)
framed_server_get_ud :: proc "c" (self: ^Framed_Server, idx: int) -> rawptr {
	if idx < 0 || idx >= len(self.inner.ud) do return nil
	#no_bounds_check {
		return self.inner.ud[idx]
	}
}

@(export)
framed_server_poll :: proc "c" (self: ^Framed_Server) {
	context = runtime.default_context()
	server.poll(&self.inner)
}

@(export)
framed_server_send_to :: proc "c" (self: ^Framed_Server, id: int, buf: [^]u8, buf_len: int) {
	context = runtime.default_context()
	server.send_to(&self.inner, id, buf[:buf_len])
}

@(export)
framed_server_send_to_all :: proc "c" (self: ^Framed_Server, buf: [^]u8, buf_len: int) {
	context = runtime.default_context()
	server.send_to_all(&self.inner, buf[:buf_len])
}

@(export)
framed_server_kick :: proc "c" (self: ^Framed_Server, id: int) {
	context = runtime.default_context()
	server.kick(&self.inner, id)
}

@(export)
framed_server_kick_all :: proc "c" (self: ^Framed_Server) {
	context = runtime.default_context()
	server.kick_many(&self.inner, nil, proc(id: int, ud: rawptr) -> bool {return true})
}

@(private = "file")
on_client_connected :: proc(inner: ^server.Server, id: int) {
	self := container_of(inner, Framed_Server, "inner")
	self.callbacks.connected(self, id)
}

@(private = "file")
on_client_disconnected :: proc(inner: ^server.Server, id: int) {
	self := container_of(inner, Framed_Server, "inner")
	self.callbacks.disconnected(self, id)
}

@(private = "file")
on_packet_received :: proc(inner: ^server.Server, id: int, buf: []u8) {
	self := container_of(inner, Framed_Server, "inner")
	self.callbacks.packet_received(self, id, raw_data(buf), len(buf))
}

@(private = "file")
on_closed :: proc(inner: ^server.Server) {
	self := container_of(inner, Framed_Server, "inner")
	self.callbacks.closed(self)
}

