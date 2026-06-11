/**
 * AuraCrypt Mega-Signaling Core Engine
 * Architecture: Event-Driven Shared-State Cluster Topology
 * Combines WhatsApp Persistence with Telegram Multi-Channel Distribution
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// Master In-Memory Database State
const MasterRegistry = {
    connectedSockets: {}, // socketId -> metadata
    activeNumbers: {},    // phone_number -> { socketId, status, identityKeys }
    channels: {},         // channelId -> { metadata, subscribers: [] }
    groups: {},           // groupId -> { metadata, members: [] }
    stories: []           // Array of global ephemeral status objects
};

console.log("[Boot] Initializing Distributed Protocol Layer...");

io.on('connection', (socket) => {
    console.log(`[Network Intercept] Socket Node Bound: ${socket.id}`);

    // Feature 1: Unique Number Allocation & Validation Engine
    socket.on('request-registration', (requestedNumber, callback) => {
        const cleanedNumber = String(requestedNumber).trim();
        const numberPattern = /^\d{3}-\d{3}-\d{3}$/;

        if (!numberPattern.test(cleanedNumber)) {
            return callback({ success: false, reason: "ERR_INVALID_FORMAT_PROTOCOL" });
        }

        if (MasterRegistry.activeNumbers[cleanedNumber]) {
            return callback({ success: false, reason: "ERR_NUMBER_COLLISION_EXISTING" });
        }

        // Allocate globally unique entry
        MasterRegistry.activeNumbers[cleanedNumber] = {
            socketId: socket.id,
            timestamp: Date.now(),
            status: "online"
        };
        
        MasterRegistry.connectedSockets[socket.id] = cleanedNumber;
        
        console.log(`[Registry Verified] Unique address secured: ${cleanedNumber}`);
        callback({ success: true, allocatedAddress: cleanedNumber });
        
        // Broadcast user status change to network
        io.emit('network-presence', { number: cleanedNumber, status: 'online' });
    });

    // Feature 2: Target Verification Pipeline
    socket.on('verify-target-exists', (targetNumber, callback) => {
        const exists = MasterRegistry.activeNumbers.hasOwnProperty(targetNumber);
        callback({ exists: exists });
    });

    // Feature 3: Core Messaging Routing Fabric (WhatsApp Ticks + Telegram Speed)
    socket.on('transmit-payload', (payload, callback) => {
        const destination = payload.targetId;
        const recipientNode = MasterRegistry.activeNumbers[destination];

        if (!recipientNode) {
            return callback({ deliveryState: "FAILED_NON_EXISTENT_ENDPOINT" });
        }

        // Route instantly to the specific websocket connection
        io.to(recipientNode.socketId).emit('incoming-payload', {
            senderId: payload.senderId,
            type: payload.type || 'text',
            content: payload.content,
            timestamp: new Date().toLocaleTimeString(),
            messageId: Math.random().toString(36).substr(2, 9),
            meta: payload.meta || {}
        });

        callback({ deliveryState: "ACKNOWLEDGED_DELIVERED" });
    });

    // Feature 4: Telegram Channels & Groups Subscription Protocol
    socket.on('join-broadcast-hub', (hubData) => {
        const { hubId, type, number } = hubData;
        socket.join(hubId);
        
        if (type === 'channel') {
            if (!MasterRegistry.channels[hubId]) {
                MasterRegistry.channels[hubId] = { subscribers: [] };
            }
            if (!MasterRegistry.channels[hubId].subscribers.includes(number)) {
                MasterRegistry.channels[hubId].subscribers.push(number);
            }
        }
    });

    socket.on('broadcast-to-hub', (broadcastPayload) => {
        // Send payload to all sockets joined to this room
        io.to(broadcastPayload.hubId).emit('hub-payload-received', broadcastPayload);
    });

    // Feature 5: Ephemeral Instagram-Style Stories Pipeline
    socket.on('publish-story', (storyData) => {
        const newStory = {
            id: Math.random().toString(36).substr(2, 9),
            author: storyData.author,
            media: storyData.media,
            caption: storyData.caption,
            timestamp: Date.now()
        };
        MasterRegistry.stories.push(newStory);
        io.emit('story-sync', MasterRegistry.stories);
    });

    // Clean up connections on disconnect
    socket.on('disconnect', () => {
        const boundNumber = MasterRegistry.connectedSockets[socket.id];
        if (boundNumber) {
            console.log(`[Registry Release] Disconnecting Node: ${boundNumber}`);
            delete MasterRegistry.activeNumbers[boundNumber];
            delete MasterRegistry.connectedSockets[socket.id];
            io.emit('network-presence', { number: boundNumber, status: 'offline' });
        }
    });
});

const PORT = 3000;
server.listen(PORT, () => {
    console.log(`====================================================`);
    console.log(`  AURACRYST MEGA INTEGRATED PROTOCOL SERVER RUNNING `);
    console.log(`  PORT: ${PORT} | Mode: Anti-Collision Registry Enabled `);
    console.log(`====================================================`);
});
