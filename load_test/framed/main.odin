package main

import fc "../../framed/client"
import fs "../../framed/server"
import "base:runtime"
import "core:fmt"
import "core:mem"
import "core:nbio"
import "core:net"
import "core:os"
import "core:time"

PORT :: 5001
INTERVAL :: 10 * time.Second
MESSAGE :: "Sometimes we just need a good networking library"
MAX_CLIENTS :: 1000
MAX_PACKET_SIZE :: 1 * mem.Kilobyte
MAX_PACKET_BACKLOG :: 16
MAX_PACKETS_PER_TICK :: 8

State :: struct {
	messages_sent:     int,
	messages_received: int,
	bytes_sent:        int,
	bytes_received:    int,
}

disconnected := MAX_CLIENTS

main :: proc() {
	assert(len(os.args) == 2, "invalid args")
	switch os.args[1] {
	case "server":
		run_server()
	case "client":
		run_clients()
	}
}

run_server :: proc() {
	nbio.acquire_thread_event_loop()
	defer nbio.release_thread_event_loop()

	server_state: State
	server: fs.Server
	_server(&server, PORT, &server_state)

	start := time.now()
	for {
		nbio.tick(0)
		fs.poll(&server)

		elapsed := time.since(start)
		if elapsed > INTERVAL {
			secs := time.duration_seconds(elapsed)

			server_bandwidth_in := int(f64(server_state.bytes_received) / secs)
			server_bandwidth_out := int(f64(server_state.bytes_sent) / secs)
			fmt.printfln(
				"[server] in: %M/s / out: %M/s / messages in: %.2f/s / messages out: %.2f/s",
				server_bandwidth_in,
				server_bandwidth_out,
				f64(server_state.messages_received) / secs,
				f64(server_state.messages_sent) / secs,
			)

			start = time.now()
			server_state = {}
		}
	}
}

_server :: proc(server: ^fs.Server, port: int, state: ^State) {
	on_client_connected :: proc(server: ^fs.Server, id: int) {}
	on_client_disconnected :: proc(server: ^fs.Server, id: int) {}
	on_packet_received :: proc(server: ^fs.Server, id: int, buf: []u8) {
		state := (^State)(server.ud[0])
		state.bytes_received += len(buf)
		state.messages_received += 1

		fs.send_to(server, id, buf)
		state.messages_sent += 1
		state.bytes_sent += len(buf)
	}

	opts := fs.Options {
		prefix_len = 2,
		endian = .Little,
		max_clients = MAX_CLIENTS,
		max_packet_size = MAX_PACKET_SIZE,
		max_packet_backlog = MAX_PACKET_BACKLOG,
		max_packets_per_tick = MAX_PACKETS_PER_TICK,
		endpoint = net.Endpoint{net.IP4_Any, port},
		callbacks = {
			client_connected = on_client_connected,
			client_disconnected = on_client_disconnected,
			packet_received = on_packet_received,
		},
	}

	server.ud[0] = state

	assert(fs.init(server, opts))
}

run_clients :: proc() {
	nbio.acquire_thread_event_loop()
	defer nbio.release_thread_event_loop()

	message := MESSAGE
	start := time.now()

	clients_state: State
	clients := new([MAX_CLIENTS]fc.Client)
	for &c, i in clients do _client(&c, PORT, &clients_state)

	for {
		nbio.tick(0)

		for &c, i in clients {
			defer fc.poll(&c)

			if !fc.is_connected(&c) do continue

			for i in 0 ..< 2 {
				fc.send(&c, transmute([]u8)(message))
				fc.is_connected(&c) or_break
				clients_state.messages_sent += 1
				clients_state.bytes_sent += len(MESSAGE)
			}
		}

		elapsed := time.since(start)
		if elapsed > INTERVAL {
			secs := time.duration_seconds(elapsed)
			clients_bandwidth_in := int(f64(clients_state.bytes_received) / secs)
			clients_bandwidth_out := int(f64(clients_state.bytes_sent) / secs)

			fmt.printfln(
				"[clients] in: %M/s / out: %M/s / messages in: %.2f/s / messages out: %.2f/s / disconnected: %v",
				clients_bandwidth_in,
				clients_bandwidth_out,
				f64(clients_state.messages_received) / secs,
				f64(clients_state.messages_sent) / secs,
				disconnected,
			)

			start = time.now()
			clients_state = {}
		}
	}
}

_client :: proc(client: ^fc.Client, port: int, state: ^State) {
	addr, _ := net.parse_ip4_address("127.0.0.1")

	on_connected :: proc(client: ^fc.Client, ok: bool) {disconnected -= 1}
	on_disconnected :: proc(client: ^fc.Client) {disconnected += 1}
	on_packet_received :: proc(client: ^fc.Client, buf: []u8) {
		state := (^State)(client.ud[0])
		state.bytes_received += len(buf)
		state.messages_received += 1
	}

	opts := fc.Options {
		prefix_len = 2,
		endian = .Little,
		max_packet_size = MAX_PACKET_SIZE,
		max_packet_backlog = MAX_PACKET_BACKLOG,
		max_packets_per_tick = MAX_PACKETS_PER_TICK,
		endpoint = net.Endpoint{addr, port},
		callbacks = {
			connected = on_connected,
			disconnected = on_disconnected,
			packet_received = on_packet_received,
		},
	}

	client.ud[0] = state

	assert(fc.init(client, opts))
}

