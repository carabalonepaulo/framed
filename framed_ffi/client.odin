package framed_ffi

import "base:runtime"
import "core:net"

import "../framed/client"
import "../framed/shared"

Framed_Client_Callbacks :: struct {
	connected:       proc "c" (server: ^Framed_Client, ok: bool),
	disconnected:    proc "c" (server: ^Framed_Client),
	packet_received: proc "c" (server: ^Framed_Client, buf: [^]u8, buf_len: int),
}

Framed_Client_Options :: struct {
	prefix_len:          int,
	endian:              shared.Endian,
	max_packet_size:     int,
	max_packet_backlog:  int,
	max_packet_per_tick: int,
	addr:                [4]u8,
	port:                u16,
	callbacks:           Framed_Client_Callbacks,
}

Framed_Client :: struct {
	inner:     client.Client,
	callbacks: Framed_Client_Callbacks,
}

@(export)
framed_client_create :: proc "c" (opts: Framed_Client_Options) -> ^Framed_Client {
	context = runtime.default_context()

	self, err := new(Framed_Client)
	if err != nil do return nil

	inner_opts := client.Options {
		prefix_len = opts.prefix_len,
		endian = opts.endian,
		max_packet_size = opts.max_packet_size,
		max_packet_backlog = opts.max_packet_backlog,
		max_packets_per_tick = opts.max_packet_per_tick,
		endpoint = net.Endpoint{net.IP4_Address(opts.addr), int(opts.port)},
		callbacks = {
			connected = on_connected,
			disconnected = on_disconnected,
			packet_received = on_packet_received,
		},
	}

	if !client.init(&self.inner, inner_opts) {
		free(self)
		return nil
	}

	self.callbacks = opts.callbacks
	return self
}

@(export)
framed_client_destroy :: proc "c" (self: ^Framed_Client) {
	context = runtime.default_context()
	client.deinit(&self.inner)
	self^ = {}
	free(self)
}

@(export)
framed_client_set_ud :: proc "c" (self: ^Framed_Client, idx: int, ud: rawptr) {
	if idx < 0 || idx >= len(self.inner.ud) do return
	#no_bounds_check {
		self.inner.ud[idx] = ud
	}
}

@(export)
framed_client_get_ud :: proc "c" (self: ^Framed_Client, idx: int) -> rawptr {
	if idx < 0 || idx >= len(self.inner.ud) do return nil
	#no_bounds_check {
		return self.inner.ud[idx]
	}
}

@(export)
framed_client_send :: proc "c" (self: ^Framed_Client, buf: [^]u8, buf_len: int) {
	context = runtime.default_context()
	client.send(&self.inner, buf[:buf_len])
}

@(export)
framed_client_poll :: proc "c" (self: ^Framed_Client) {
	context = runtime.default_context()
	client.poll(&self.inner)
}

@(export)
framed_client_close :: proc "c" (self: ^Framed_Client) {
	context = runtime.default_context()
	client.close(&self.inner)
}

@(private = "file")
on_connected :: proc(inner: ^client.Client, ok: bool) {
	self := container_of(inner, Framed_Client, "inner")
	self.callbacks.connected(self, ok)
}

@(private = "file")
on_disconnected :: proc(inner: ^client.Client) {
	self := container_of(inner, Framed_Client, "inner")
	self.callbacks.disconnected(self)
}

@(private = "file")
on_packet_received :: proc(inner: ^client.Client, buf: []u8) {
	self := container_of(inner, Framed_Client, "inner")
	self.callbacks.packet_received(self, raw_data(buf), len(buf))
}

