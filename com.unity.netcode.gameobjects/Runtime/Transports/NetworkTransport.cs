using System;
using System.Collections.Generic;
using Cysharp.Threading.Tasks;
using UnityEngine;

namespace Unity.Netcode
{
    public abstract class NetworkTransport : MonoBehaviour
    {
        /// <summary>
        /// A constant `clientId` that represents the server
        /// When this value is found in methods such as `Send`, it should be treated as a placeholder that means "the server"
        /// </summary>
        public abstract ulong ServerClientId { get; }

        /// <summary>
        /// Gets a value indicating whether this <see cref="T:NetworkTransport"/> is supported in the current runtime context
        /// This is used by multiplex adapters
        /// </summary>
        /// <value><c>true</c> if is supported; otherwise, <c>false</c>.</value>
        public virtual bool IsSupported => true;

        internal INetworkMetrics NetworkMetrics;

        // KEEPSAKE FIX
        public abstract void GetPipelineStats(ref List<PipelineStats> stats);
        public abstract void GetDriverStats(ref DriverStats stats);
        public abstract void SetLatencyTraceId(int latencyTraceId);
        public abstract void ProcessLatencyTrace();
        // END KEEPSAKE FIX

        /// <summary>
        /// Delegate for transport network events
        /// </summary>
        public delegate void TransportEventDelegate(NetworkEvent eventType, ulong clientId, ArraySegment<byte> payload, float receiveTime);

        /// <summary>
        /// Occurs when the transport has a new transport network event.
        /// Can be used to make an event based transport instead of a poll based.
        /// Invocation has to occur on the Unity thread in the Update loop.
        /// </summary>
        public event TransportEventDelegate OnTransportEvent;

        /// <summary>
        /// Invokes the <see cref="OnTransportEvent"/>. Invokation has to occur on the Unity thread in the Update loop.
        /// </summary>
        /// <param name="eventType">The event type</param>
        /// <param name="clientId">The clientId this event is for</param>
        /// <param name="payload">The incoming data payload</param>
        /// <param name="receiveTime">The time the event was received, as reported by Time.realtimeSinceStartup.</param>
        protected void InvokeOnTransportEvent(NetworkEvent eventType, ulong clientId, ArraySegment<byte> payload, float receiveTime)
        {
            OnTransportEvent?.Invoke(eventType, clientId, payload, receiveTime);
        }

        /// <summary>
        /// Send a payload to the specified clientId, data and channelName.
        /// </summary>
        /// <param name="clientId">The clientId to send to</param>
        /// <param name="payload">The data to send</param>
        /// <param name="networkDelivery">The delivery type (QoS) to send data with</param>
        public abstract void Send(ulong clientId, ArraySegment<byte> payload, NetworkDelivery networkDelivery);

        /// <summary>
        /// Polls for incoming events, with an extra output parameter to report the precise time the event was received.
        /// </summary>
        /// <param name="clientId">The clientId this event is for</param>
        /// <param name="payload">The incoming data payload</param>
        /// <param name="receiveTime">The time the event was received, as reported by Time.realtimeSinceStartup.</param>
        /// <returns>Returns the event type</returns>
        public abstract NetworkEvent PollEvent(out ulong clientId, out ArraySegment<byte> payload, out float receiveTime);

        /// <summary>
        /// Connects client to the server
        /// </summary>
        public abstract bool StartClient();

        /// <summary>
        /// Starts to listening for incoming clients
        /// </summary>
        public abstract bool StartServer();

        /// <summary>
        /// Disconnects a client from the server
        /// </summary>
        /// <param name="clientId">The clientId to disconnect</param>
        /// <param name="graceful">KEEPSAKE FIX</param>
        public abstract void DisconnectRemoteClient(ulong clientId, bool graceful = true);

        /// <summary>
        /// Disconnects the local client from the server
        /// </summary>
        public abstract void DisconnectLocalClient();

        /// <summary>
        /// Gets the round trip time for a specific client. This method is optional
        /// </summary>
        /// <param name="clientId">The clientId to get the RTT from</param>
        /// <returns>Returns the round trip time in milliseconds </returns>
        public abstract ulong GetCurrentRtt(ulong clientId);

        // KEEPSAKE FIX
        public virtual async UniTask PrepareGracefulShutdownAsync()
        {
            await UniTask.CompletedTask;
        }

        /// <summary>
        /// Shuts down the transport
        /// </summary>
        public abstract void Shutdown();

        /// <summary>
        /// Initializes the transport
        /// </summary>
        public abstract void Initialize();
    }

    // KEEPSAKE FIX
    public struct PipelineStats
    {
        public const int SendQueueSize = 32; // keep in sync with ReliableUtility

        public bool Reliable;
        public bool Sequenced;

        public              int   TotalOutboundPacketsEntered;
        public              int   TotalOutboundPacketsExited;
        public              int   TotalOutboundPacketsExitedResend; // for reliable pipes
        public unsafe fixed ulong OutboundPacketsEntered[128];      // see StatsUtilities.MaxPacketsTrackedPerFlush
        public              int   OutboundPacketsEnteredCount;
        public unsafe fixed ulong OutboundPacketsExited[128];
        public              int   OutboundPacketsExitedCount;
        public unsafe fixed ulong OutboundPacketsExitedResend[128];       // for reliable pipes
        public              int   OutboundPacketsExitedResendCount;       // for reliable pipes
        public              bool  OutboundPacketsRejectedQueueFull;       // for reliable pipes
        public unsafe fixed int   OutboundPacketSendQueue[SendQueueSize]; // for reliable pipes

        public              int   TotalInboundPacketsEntered;
        public              int   TotalInboundPacketsExited;
        public unsafe fixed ulong InboundPacketsEntered[128];
        public              int   InboundPacketsEnteredCount;
        public              int   InboundPacketAcksCount;
        public unsafe fixed ulong InboundPacketsExited[128];
        public              int   InboundPacketsExitedCount;
    }

    public struct SendQueueStats
    {
        public string Name;
        public int    Length;
        public int    Consumed;
        public int    Added;
        public int    SendSuccess;
        public int    SendError;
    }

    public struct DriverStats
    {
        public int SentPackets;
        public int ReceivedPackets;

        public SendQueueStats[] SendQueues;
    }
    // END KEEPSAKE FIX
}
