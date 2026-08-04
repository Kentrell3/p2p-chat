const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

console.log(`🚀 Starting server on port ${PORT}...`);

const server = http.createServer((req, res) => {
    fs.readFile('index.html', (err, data) => {
        if (err) {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('✅ Server is running! WebSocket is active.');
        } else {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        }
    });
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

wss.on('connection', (ws) => {
    console.log('🔌 New WebSocket connection');
    let peerId = null;
    let roomId = null;
    let username = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log(`📨 Received: ${msg.type} from ${username || 'unknown'}`);

            switch(msg.type) {
                case 'register':
                    roomId = msg.roomId;
                    peerId = msg.peerId;
                    username = msg.username;
                    
                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, new Map());
                    }
                    const room = rooms.get(roomId);
                    room.set(peerId, { ws, username });
                    
                    const existingPeers = Array.from(room.entries())
                        .filter(([id]) => id !== peerId)
                        .map(([id, info]) => ({ 
                            peerId: id, 
                            username: info.username 
                        }));
                    
                    console.log(`✅ ${username} registered in room ${roomId}`);
                    console.log(`📊 Room has ${room.size} peers`);
                    
                    ws.send(JSON.stringify({
                        type: 'registered',
                        peerId: peerId,
                        existingPeers: existingPeers
                    }));
                    
                    broadcastToRoom(roomId, {
                        type: 'peer-joined',
                        peerId: peerId,
                        username: username
                    }, [peerId]);
                    break;
                    
                case 'offer':
                    console.log(`📤 Forwarding offer from ${username} to ${msg.targetPeerId}`);
                    forwardToPeer(msg.targetPeerId, {
                        type: 'offer',
                        from: peerId,
                        username: username,
                        sdp: msg.sdp
                    });
                    break;
                    
                case 'answer':
                    console.log(`📤 Forwarding answer from ${username} to ${msg.targetPeerId}`);
                    forwardToPeer(msg.targetPeerId, {
                        type: 'answer',
                        from: peerId,
                        sdp: msg.sdp
                    });
                    break;
                    
                case 'candidate':
                    console.log(`📤 Forwarding ICE candidate from ${username} to ${msg.targetPeerId}`);
                    forwardToPeer(msg.targetPeerId, {
                        type: 'candidate',
                        from: peerId,
                        candidate: msg.candidate
                    });
                    break;
                    
                case 'file-offer':
                case 'file-answer':
                case 'file-candidate':
                    forwardToPeer(msg.targetPeerId, {
                        type: msg.type,
                        from: peerId,
                        username: username,
                        fileId: msg.fileId,
                        fileName: msg.fileName,
                        fileSize: msg.fileSize,
                        fileType: msg.fileType
                    });
                    break;
                    
                case 'call-offer':
                case 'call-answer':
                case 'call-candidate':
                    forwardToPeer(msg.targetPeerId, {
                        type: msg.type,
                        from: peerId,
                        username: username,
                        sdp: msg.sdp,
                        mediaType: msg.mediaType
                    });
                    break;
                    
                case 'peer-disconnect':
                    console.log(`👋 ${username} disconnecting`);
                    handleDisconnect(peerId, roomId);
                    break;
                    
                default:
                    console.log(`❓ Unknown message type: ${msg.type}`);
            }
        } catch(e) {
            console.error('❌ Error processing message:', e);
        }
    });
    
    ws.on('close', () => {
        console.log(`🔌 WebSocket closed for ${peerId}`);
        handleDisconnect(peerId, roomId);
    });
});

function broadcastToRoom(roomId, message, exclude = []) {
    if (!rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    room.forEach((info, id) => {
        if (!exclude.includes(id) && info.ws.readyState === WebSocket.OPEN) {
            info.ws.send(JSON.stringify(message));
        }
    });
}

function forwardToPeer(targetPeerId, message) {
    console.log(`🔍 Looking for peer: ${targetPeerId}`);
    for (const [roomId, room] of rooms) {
        if (room.has(targetPeerId)) {
            const info = room.get(targetPeerId);
            if (info.ws.readyState === WebSocket.OPEN) {
                console.log(`✅ Forwarding to ${targetPeerId}`);
                info.ws.send(JSON.stringify(message));
                return true;
            } else {
                console.log(`❌ Peer ${targetPeerId} websocket not open`);
            }
        }
    }
    console.log(`❌ Peer ${targetPeerId} not found in any room`);
    return false;
}

function handleDisconnect(peerId, roomId) {
    if (roomId && rooms.has(roomId)) {
        const room = rooms.get(roomId);
        room.delete(peerId);
        broadcastToRoom(roomId, {
            type: 'peer-left',
            peerId: peerId
        });
        if (room.size === 0) rooms.delete(roomId);
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`🔗 WebSocket: ws://0.0.0.0:${PORT}`);
});
