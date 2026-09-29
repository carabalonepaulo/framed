package main

import "core:fmt"
import "core:os"
import "core:time"
import enet "vendor:ENet"

MESSAGE :: "Sometimes we just need a good networking library"
MAX_CLIENTS :: 1000
PORT :: 5001
INTERVAL :: 10 * time.Second

State :: struct {
	messages_sent:     int,
	messages_received: int,
	bytes_sent:        int,
	bytes_received:    int,
}

disconnected := MAX_CLIENTS

main :: proc() {
	if enet.initialize() != 0 {
		fmt.println("Falha ao inicializar o ENet")
		return
	}
	defer enet.deinitialize()

	assert(len(os.args) == 2, "invalid args")
	switch os.args[1] {
	case "server":
		run_server()
	case "client":
		run_clients()
	}
}

run_server :: proc() {
	server_address := enet.Address {
		host = enet.HOST_ANY,
		port = PORT,
	}

	server_host := enet.host_create(
		address = &server_address,
		peerCount = MAX_CLIENTS,
		channelLimit = 1,
		incomingBandwidth = 0,
		outgoingBandwidth = 0,
	)
	if server_host == nil {
		fmt.println("Falha ao criar o host do servidor ENet")
		return
	}
	defer enet.host_destroy(server_host)

	server_state: State
	start := time.now()
	message_bytes := transmute([]u8)(string(MESSAGE))

	for {
		event: enet.Event
		for enet.host_service(server_host, &event, 0) > 0 {
			#partial switch event.type {
			case .RECEIVE:
				server_state.bytes_received += int(event.packet.dataLength)
				server_state.messages_received += 1

				response := enet.packet_create(
					event.packet.data,
					event.packet.dataLength,
					{.RELIABLE},
				)
				enet.peer_send(event.peer, 0, response)

				server_state.bytes_sent += int(event.packet.dataLength)
				server_state.messages_sent += 1

				enet.packet_destroy(event.packet)
			case .CONNECT, .DISCONNECT:
			}
		}

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

run_clients :: proc() {
	client_hosts := make([]^enet.Host, MAX_CLIENTS)
	defer delete(client_hosts)

	peers := make([]^enet.Peer, MAX_CLIENTS)
	defer delete(peers)

	client_address: enet.Address
	enet.address_set_host(&client_address, "127.0.0.1")
	client_address.port = PORT

	for i in 0 ..< MAX_CLIENTS {
		c_host := enet.host_create(
			address = nil,
			peerCount = 1,
			channelLimit = 1,
			incomingBandwidth = 0,
			outgoingBandwidth = 0,
		)
		if c_host == nil {
			fmt.println("Falha ao criar o host do cliente:", i)
			return
		}
		client_hosts[i] = c_host

		peer := enet.host_connect(c_host, &client_address, 1, 0)
		if peer == nil {
			fmt.println("Falha ao iniciar conexão para o cliente:", i)
			return
		}
		peers[i] = peer
	}

	defer {
		for h in client_hosts {
			if h != nil do enet.host_destroy(h)
		}
	}

	clients_state: State
	start := time.now()
	message_bytes := transmute([]u8)(string(MESSAGE))

	for {
		event: enet.Event
		for i in 0 ..< MAX_CLIENTS {
			c_host := client_hosts[i]
			peer := peers[i]

			for enet.host_service(c_host, &event, 0) > 0 {
				#partial switch event.type {
				case .CONNECT:
					disconnected -= 1
				case .DISCONNECT:
					disconnected += 1
				case .RECEIVE:
					clients_state.bytes_received += int(event.packet.dataLength)
					clients_state.messages_received += 1
					enet.packet_destroy(event.packet)
				}
			}

			if peer.state == .CONNECTED {
				for _ in 0 ..< 2 {
					packet := enet.packet_create(
						raw_data(message_bytes),
						len(message_bytes),
						{.RELIABLE},
					)
					if enet.peer_send(peer, 0, packet) == 0 {
						clients_state.messages_sent += 1
						clients_state.bytes_sent += len(MESSAGE)
					} else {
						enet.packet_destroy(packet)
					}
				}
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

