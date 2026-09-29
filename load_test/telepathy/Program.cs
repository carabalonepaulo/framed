using System.Diagnostics;
using System.Text;
using Telepathy;

namespace LoadTest;

public class Program
{
    private const string MESSAGE = "Sometimes we just need a good networking library";
    private const int MAX_CLIENTS = 1000;
    private const int PORT = 5001;
    private const int MAX_PACKET_SIZE = 1024;
    private static readonly TimeSpan INTERVAL = TimeSpan.FromSeconds(10);

    private struct State
    {
        public long MessagesSent;
        public long MessagesReceived;
        public long BytesSent;
        public long BytesReceived;
    }

    private static int disconnected = MAX_CLIENTS;

    public static void Main(string[] args)
    {
        if (args[0] == "server") {
            RunServer();
        } else if (args[0] == "client") {
            RunClients();
        }
    }

    static void RunServer()
    {
        var server = new Server(MAX_PACKET_SIZE);

        State serverState = default;
        server.OnData = (connectionId, data) =>
        {
            serverState.BytesReceived += data.Count;
            serverState.MessagesReceived++;

            server.Send(connectionId, data);
            serverState.MessagesSent++;
            serverState.BytesSent += data.Count;
        };

        server.Start(PORT);

        Stopwatch stopwatch = Stopwatch.StartNew();

        while (true)
        {
            server.Tick(100000);

            TimeSpan elapsed = stopwatch.Elapsed;
            if (elapsed >= INTERVAL)
            {
                double secs = elapsed.TotalSeconds;

                long serverBandwidthIn = (long)(serverState.BytesReceived / secs);
                long serverBandwidthOut = (long)(serverState.BytesSent / secs);

                Console.WriteLine(
                    $"[server] in: {FormatHumanReadableBytes(serverBandwidthIn)}/s / out: {FormatHumanReadableBytes(serverBandwidthOut)}/s / messages in: {serverState.MessagesReceived / secs:F2}/s / messages out: {serverState.MessagesSent / secs:F2}/s"
                );

                stopwatch.Restart();
                serverState = default;
            }
        }
    }

    static void RunClients()
    {
        byte[] messageBytes = Encoding.UTF8.GetBytes(MESSAGE);
        var messageSegment = new ArraySegment<byte>(messageBytes);

        State clientsState = default;
        var clients = new Client[MAX_CLIENTS];

        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            var client = new Client(MAX_PACKET_SIZE);

            client.OnConnected = () => Interlocked.Decrement(ref disconnected);
            client.OnDisconnected = () => Interlocked.Increment(ref disconnected);
            client.OnData = (data) =>
            {
                clientsState.BytesReceived += data.Count;
                clientsState.MessagesReceived++;
            };

            client.Connect("127.0.0.1", PORT);
            clients[i] = client;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        while (true)
        {
            Thread.Sleep(1); // server crashes without this

            for (int i = 0; i < MAX_CLIENTS; i++)
            {
                var client = clients[i];

                client.Tick(10000);

                if (client.Connected)
                {
                    for (int j = 0; j < 2; j++)
                    {
                        if (client.Send(messageSegment))
                        {
                            clientsState.MessagesSent++;
                            clientsState.BytesSent += messageBytes.Length;
                        }
                    }
                }
            }

            TimeSpan elapsed = stopwatch.Elapsed;
            if (elapsed >= INTERVAL)
            {
                double secs = elapsed.TotalSeconds;

                long clientsBandwidthIn = (long)(clientsState.BytesReceived / secs);
                long clientsBandwidthOut = (long)(clientsState.BytesSent / secs);

                Console.WriteLine(
                    $"[clients] in: {FormatHumanReadableBytes(clientsBandwidthIn)}/s / out: {FormatHumanReadableBytes(clientsBandwidthOut)}/s / messages in: {clientsState.MessagesReceived / secs:F2}/s / messages out: {clientsState.MessagesSent / secs:F2}/s / disconnected: {Volatile.Read(ref disconnected)}"
                );

                stopwatch.Restart();
                clientsState = default;
            }
        }
    }

    private static string FormatHumanReadableBytes(long bytes)
    {
        if (bytes >= 1024 * 1024 * 1024)
            return $"{(double)bytes / (1024 * 1024 * 1024):F2} GiB";
        if (bytes >= 1024 * 1024)
            return $"{(double)bytes / (1024 * 1024):F2} MiB";
        if (bytes >= 1024)
            return $"{(double)bytes / 1024:F2} KiB";

        return $"{bytes} B";
    }
}
