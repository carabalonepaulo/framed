package framed_client

import "core:mem"
import "core:nbio"
import "core:net"
import "core:time"

import cb "../circular_buffer"
import "../shared"

Callbacks :: struct {
	connected:       proc(self: ^Client, ok: bool),
	disconnected:    proc(self: ^Client),
	packet_received: proc(self: ^Client, buf: []u8),
}

Options :: struct {
	prefix_len:           int,
	endian:               shared.Endian,
	max_packet_size:      int,
	max_packet_backlog:   int,
	max_packets_per_tick: int,
	endpoint:             net.Endpoint,
	connect_timeout:      time.Duration,
	callbacks:            Callbacks,
}

@(private = "file")
Flag :: enum {
	Sending,
	Receiving,
	Close_Later,
	Close_Now,
	Disconnected,
}

@(private = "file")
Flags :: bit_set[Flag]

Client :: struct {
	sock:                 nbio.TCP_Socket,
	expecting:            int,
	flags:                Flags,
	callbacks:            Callbacks,
	ud:                   [shared.MAX_USER_ARGUMENTS]rawptr,
	buf:                  []u8,
	temp:                 []u8,
	send_buf:             cb.Circular_Buffer,
	recv_buf:             cb.Circular_Buffer,
	connect_timeout:      time.Duration,
	prefix_len:           int,
	endian:               shared.Endian,
	max_packets_per_tick: int,
}

init :: proc(self: ^Client, opts: Options) -> (ok: bool) {
	if opts.callbacks.connected == nil ||
	   opts.callbacks.disconnected == nil ||
	   opts.callbacks.packet_received == nil ||
	   (opts.prefix_len < 1 || opts.prefix_len > 8) {
		return false
	}

	buf_size := opts.max_packet_size * opts.max_packet_backlog
	buf, err := make([]u8, opts.max_packet_size + buf_size * 2)
	if err != nil do return false

	arena: mem.Arena
	mem.arena_init(&arena, buf)
	arena_alloc := mem.arena_allocator(&arena)

	self.prefix_len = opts.prefix_len == 0 ? shared.DEFAULT_PREFIX_LEN : opts.prefix_len
	self.endian = opts.endian
	self.flags = {.Disconnected}
	self.callbacks = opts.callbacks
	self.expecting = -1
	self.buf = buf
	self.temp = make([]u8, opts.max_packet_size, arena_alloc)
	self.send_buf = cb.create(make([]u8, buf_size, arena_alloc))
	self.recv_buf = cb.create(make([]u8, buf_size, arena_alloc))
	self.max_packets_per_tick = opts.max_packets_per_tick

	self.connect_timeout = opts.connect_timeout == 0 ? nbio.NO_TIMEOUT : opts.connect_timeout

	op := nbio.dial(opts.endpoint, on_dial, self.connect_timeout)
	op.user_data[0] = self

	return true
}

deinit :: proc(self: ^Client) {
	delete(self.buf)
}

poll :: proc(self: ^Client) {
	if .Disconnected in self.flags do return

	is_force := .Close_Now in self.flags
	is_drain := .Close_Later in self.flags && self.send_buf.ra == 0

	if (is_force || is_drain) {
		net.shutdown(self.sock, .Both)
		nbio.close(self.sock)
		self.flags += {.Disconnected}
		self.callbacks.disconnected(self)
		return
	}

	if .Close_Now in self.flags do return

	if .Sending not_in self.flags {
		buf := cb.peek_read(&self.send_buf)
		if len(buf) > 0 {
			op := nbio.send(self.sock, {buf}, on_send)
			op.user_data[0] = self
			self.flags += {.Sending}
		}
	}

	if self.flags & {.Close_Later, .Receiving} == {} {
		buf := cb.peek_write(&self.recv_buf)
		if len(buf) > 0 {
			self.flags += {.Receiving}
			op := nbio.recv(self.sock, {buf}, on_recv)
			op.user_data[0] = self
		}
	}

	for _ in 0 ..< self.max_packets_per_tick {
		buf, overflow, ok := shared.try_read(
			&self.expecting,
			&self.recv_buf,
			self.temp,
			self.prefix_len,
			self.endian,
		)
		if !ok {
			if overflow do self.flags += {.Close_Now}
			break
		}
		self.callbacks.packet_received(self, buf)
	}
}

send :: proc(self: ^Client, buf: []u8) {
	if self.flags & {.Close_Now, .Close_Later} != {} do return
	ok := shared.try_write(&self.send_buf, buf, self.prefix_len, self.endian)
	if !ok do self.flags += {.Close_Later}
}

close :: proc(self: ^Client) {
	self.flags += {.Close_Later}
}

is_connected :: #force_inline proc(self: ^Client) -> bool {
	return self.flags & {.Close_Now, .Close_Later, .Disconnected} == {}
}

@(private = "file")
on_dial :: proc(op: ^nbio.Operation) {
	self := (^Client)(op.user_data[0])
	defer self.callbacks.connected(self, op.dial.err == nil)
	if op.dial.err != nil do return

	self.sock = op.dial.socket
	self.flags += {.Receiving}
	self.flags -= {.Disconnected}
	cb.clear(&self.send_buf)
	cb.clear(&self.recv_buf)

	op := nbio.recv(self.sock, {cb.peek_write(&self.recv_buf)}, on_recv)
	op.user_data[0] = self
}

@(private = "file")
on_recv :: proc(op: ^nbio.Operation) {
	self := (^Client)(op.user_data[0])
	self.flags -= {.Receiving}

	if op.recv.err != nil || op.recv.received == 0 {
		self.flags += {.Close_Now}
		return
	}

	cb.commit_write(&self.recv_buf, op.recv.received)

	write_buf := cb.peek_write(&self.recv_buf)
	if len(write_buf) > 0 {
		self.flags += {.Receiving}
		op := nbio.recv(self.sock, {write_buf}, on_recv)
		op.user_data[0] = self
	}
}

@(private = "file")
on_send :: proc(op: ^nbio.Operation) {
	self := (^Client)(op.user_data[0])
	self.flags -= {.Sending}

	if op.send.err == nil do cb.commit_read(&self.send_buf, op.send.sent)
	else do self.flags += {.Close_Now}
}

