package framed_server

import "base:intrinsics"
import "base:runtime"
import "core:container/intrusive/list"
import "core:mem"
import "core:nbio"
import "core:net"

import cb "../circular_buffer"
import "../shared"

INDEX_BITS :: 16
INDEX_MASK :: (1 << INDEX_BITS) - 1

Callbacks :: struct {
	client_connected:    proc(self: ^Server, id: int),
	client_disconnected: proc(self: ^Server, id: int),
	packet_received:     proc(self: ^Server, id: int, buf: []u8),
	closed:              proc(self: ^Server),
}

Options :: struct {
	prefix_len:           int,
	endian:               shared.Endian,
	max_clients:          int,
	max_packet_size:      int,
	max_packet_backlog:   int,
	max_packets_per_tick: int,
	endpoint:             net.Endpoint,
	callbacks:            Callbacks,
}

@(private = "file")
Client_Flag :: enum {
	Sending,
	Receiving,
	Close_Later,
	Close_Now,
}

@(private = "file")
Client_Flags :: bit_set[Client_Flag]

@(private = "file")
Client :: struct {
	id:        int,
	expecting: int,
	sock:      nbio.TCP_Socket,
	flags:     Client_Flags,
	node:      list.Node,
	send_buf:  cb.Circular_Buffer,
	recv_buf:  cb.Circular_Buffer,
}

@(private = "file")
Server_State :: enum {
	Active,
	Shutting_Down,
	Closing,
	Closed,
}

Server :: struct {
	sock:                 nbio.TCP_Socket,
	state:                Server_State,
	used_list:            list.List,
	free_list:            list.List,
	temp:                 []u8,
	clients_buf:          []u8,
	clients:              []Client,
	callbacks:            Callbacks,
	ud:                   [shared.MAX_USER_ARGUMENTS]rawptr,
	prefix_len:           int,
	endian:               shared.Endian,
	connected_count:      int,
	max_packets_per_tick: int,
}

init :: proc(self: ^Server, opts: Options) -> (ok: bool) {
	if opts.callbacks.client_connected == nil ||
	   opts.callbacks.client_disconnected == nil ||
	   opts.callbacks.packet_received == nil ||
	   (opts.prefix_len < 1 || opts.prefix_len > 8) {
		return false
	}

	sock, listen_err := nbio.listen_tcp(opts.endpoint)
	if listen_err != nil do return false

	buf_size := opts.max_packet_size * opts.max_packet_backlog
	total_size := (opts.max_clients * buf_size * 2) + opts.max_packet_size

	runtime.is_power_of_two(buf_size) or_return

	clients_buf, buf_err := make([]u8, total_size)
	if buf_err != nil do return false
	defer if !ok do delete(clients_buf)

	clients, clients_err := make([]Client, opts.max_clients)
	if clients_err != nil do return false
	defer if !ok do delete(clients)

	arena: mem.Arena
	mem.arena_init(&arena, clients_buf)
	arena_alloc := mem.arena_allocator(&arena)

	for &c, i in clients {
		c.id = pack_key(i, 1)
		c.expecting = -1
		c.send_buf = cb.create(make([]u8, buf_size, arena_alloc))
		c.recv_buf = cb.create(make([]u8, buf_size, arena_alloc))

		list.push_back(&self.free_list, &c.node)
	}

	self.prefix_len = opts.prefix_len == 0 ? shared.DEFAULT_PREFIX_LEN : opts.prefix_len
	self.endian = opts.endian
	self.callbacks = opts.callbacks
	self.sock = sock
	self.clients = clients
	self.clients_buf = clients_buf
	self.temp = make([]u8, opts.max_packet_size, arena_alloc)
	self.max_packets_per_tick = opts.max_packets_per_tick

	op := nbio.accept(self.sock, on_accept)
	op.user_data[0] = self

	return true
}

deinit :: proc(self: ^Server) {
	delete(self.clients_buf)
	delete(self.clients)
}

close :: proc(self: ^Server) {
	if self.connected_count == 0 {
		_close(self)
	} else {
		self.state = .Shutting_Down
		kick_all(self)
	}
}

poll :: proc(self: ^Server) {
	curr := self.used_list.head
	for curr != nil {
		next := curr.next

		client := container_of(curr, Client, "node")

		is_force := .Close_Now in client.flags
		is_drain := .Close_Later in client.flags && client.send_buf.ra == 0

		if is_force || is_drain {
			net.shutdown(client.sock, .Both)
			nbio.close(client.sock)

			list.remove(&self.used_list, &client.node)
			list.push_back(&self.free_list, &client.node)

			old_id := client.id
			c_idx, c_gen := unpack_key(client.id)
			client.id = pack_key(c_idx, c_gen + 1)
			client.flags = {}
			cb.clear(&client.send_buf)
			cb.clear(&client.recv_buf)

			self.connected_count -= 1
			self.callbacks.client_disconnected(self, old_id)

			if self.connected_count == 0 {
				#partial switch self.state {
				case .Shutting_Down:
					_close(self)
				case .Closing:
					self.state = .Closed
					self.callbacks.closed(self)
				}
			}

			curr = next
			continue
		}

		if .Close_Now in client.flags {
			curr = next
			continue
		}

		if .Sending not_in client.flags {
			buf := cb.peek_read(&client.send_buf)
			if len(buf) > 0 {
				op := nbio.send(client.sock, {buf}, on_send)
				op.user_data[0] = self
				op.user_data[1] = transmute(rawptr)(client.id)
				client.flags += {.Sending}
			}
		}

		if client.flags & {.Close_Later, .Receiving} == {} {
			buf := cb.peek_write(&client.recv_buf)
			if len(buf) > 0 {
				client.flags += {.Receiving}
				op := nbio.recv(client.sock, {buf}, on_recv)
				op.user_data[0] = self
				op.user_data[1] = transmute(rawptr)(client.id)
			}
		}

		for _ in 0 ..< self.max_packets_per_tick {
			buf, overflow, ok := shared.try_read(
				&client.expecting,
				&client.recv_buf,
				self.temp,
				self.prefix_len,
				self.endian,
			)
			if !ok {
				if overflow do client.flags += {.Close_Now}
				break
			}
			self.callbacks.packet_received(self, client.id, buf)
		}

		curr = next
	}
}

send_to :: proc(self: ^Server, id: int, buf: []u8) {
	client := get_client(self.clients, id)
	if client == nil do return
	if client.flags & {.Close_Now, .Close_Later} != {} do return
	write(self, client, buf)
}

send_to_many :: proc(
	self: ^Server,
	buf: []u8,
	ud: rawptr,
	filter: proc(id: int, ud: rawptr) -> bool,
) {
	curr := self.used_list.head
	for curr != nil {
		client := container_of(curr, Client, "node")
		active := client.flags & {.Close_Now, .Close_Later} == {}
		if active && filter(client.id, ud) do write(self, client, buf)
		curr = curr.next
	}
}

send_to_all :: proc(self: ^Server, buf: []u8) {
	curr := self.used_list.head
	for curr != nil {
		client := container_of(curr, Client, "node")
		active := client.flags & {.Close_Now, .Close_Later} == {}
		if active do write(self, client, buf)
		curr = curr.next
	}
}

kick :: proc(self: ^Server, id: int) {
	client := get_client(self.clients, id)
	if client != nil do client.flags += {.Close_Later}
}

kick_many :: proc(self: ^Server, ud: rawptr, filter: proc(id: int, ud: rawptr) -> bool) {
	curr := self.used_list.head
	for curr != nil {
		client := container_of(curr, Client, "node")
		if filter(client.id, ud) do client.flags += {.Close_Later}
		curr = curr.next
	}
}

kick_all :: proc(self: ^Server) {
	curr := self.used_list.head
	for curr != nil {
		client := container_of(curr, Client, "node")
		client.flags += {.Close_Later}
		curr = curr.next
	}
}

client_count :: #force_inline proc(self: ^Server) -> int {
	return self.connected_count
}

is_closed :: #force_inline proc(self: ^Server) -> bool {
	return self.state == .Closed
}

@(private = "file")
on_accept :: proc(op: ^nbio.Operation) {
	self := (^Server)(op.user_data[0])
	should_accept := true
	defer if should_accept {
		op := nbio.accept(self.sock, on_accept)
		op.user_data[0] = self
	}

	#partial switch op.accept.err {
	case .None:
		should_accept = true
	case .Aborted, .Insufficient_Resources, .Interrupted:
		should_accept = true
		return
	case:
		should_accept = false
		return
	}

	if self.state != .Active {
		should_accept = false
		nbio.close(op.accept.client)
		return
	}

	node := list.pop_front(&self.free_list)
	if node == nil {
		nbio.close(op.accept.client)
		return
	}
	list.push_back(&self.used_list, node)

	client := container_of(node, Client, "node")
	client.expecting = -1
	client.sock = op.accept.client
	client.flags = {.Receiving}
	cb.clear(&client.send_buf)
	cb.clear(&client.recv_buf)

	op := nbio.recv(client.sock, {cb.peek_write(&client.recv_buf)}, on_recv)
	op.user_data[0] = self
	op.user_data[1] = transmute(rawptr)(client.id)

	self.connected_count += 1
	self.callbacks.client_connected(self, client.id)
}

@(private = "file")
on_recv :: proc(op: ^nbio.Operation) {
	self := (^Server)(op.user_data[0])
	id := transmute(int)(op.user_data[1])

	client := get_client(self.clients, id)
	if client == nil do return
	client.flags -= {.Receiving}

	if op.recv.err != nil || op.recv.received == 0 {
		client.flags += {.Close_Now}
		return
	}

	cb.commit_write(&client.recv_buf, op.recv.received)

	write_buf := cb.peek_write(&client.recv_buf)
	if len(write_buf) > 0 {
		client.flags += {.Receiving}
		op := nbio.recv(client.sock, {write_buf}, on_recv)
		op.user_data[0] = self
		op.user_data[1] = transmute(rawptr)(client.id)
	}
}

@(private = "file")
on_send :: proc(op: ^nbio.Operation) {
	self := (^Server)(op.user_data[0])
	id := transmute(int)(op.user_data[1])

	client := get_client(self.clients, id)
	if client == nil do return
	client.flags -= {.Sending}

	if op.send.err == nil do cb.commit_read(&client.send_buf, op.send.sent)
	else do client.flags += {.Close_Now}
}

@(private = "file")
on_close :: proc(op: ^nbio.Operation) {
	self := (^Server)(op.user_data[0])
	if self.connected_count == 0 && self.state == .Closing {
		self.state = .Closed
		self.callbacks.closed(self)
	}
}

@(private = "file")
pack_key :: #force_inline proc(idx: int, gen: int) -> int {
	return ((gen & INDEX_MASK) << INDEX_BITS) | (idx & INDEX_MASK)
}

@(private = "file")
unpack_key :: #force_inline proc(key: int) -> (idx: int, gen: int) {
	return key & INDEX_MASK, (key >> INDEX_BITS) & INDEX_MASK
}

@(private = "file")
get_client :: #force_inline proc(clients: []Client, id: int) -> (client: ^Client) {
	idx, gen := unpack_key(id)
	if idx < 0 || idx >= len(clients) do return nil

	#no_bounds_check {
		client = &clients[idx]
	}

	c_gen := (client.id >> INDEX_BITS) & INDEX_MASK
	if gen != c_gen do return nil

	return client
}

@(private = "file")
write :: #force_inline proc(self: ^Server, client: ^Client, buf: []u8) {
	ok := shared.try_write(&client.send_buf, buf, self.prefix_len, self.endian)
	if !ok do client.flags += {.Close_Later}
}

@(private = "file")
_close :: proc(self: ^Server) {
	self.state = .Closing
	op := nbio.close(self.sock, on_close)
	op.user_data[0] = self
}

