// server.js - COMPLETE FIX WITH USER TRACKING
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

// ===== STORAGE =====
const publicMessages = {};
const privateMessages = {};
// Track users by username (not connection ID)
const clients = {}; // roomId -> { username: ws }
// Track user connections per room
const userConnections = {}; // roomId -> { username: { ws, connected: true } }

const server = http.createServer((req, res) => {
    fs.readFile('index.html', (err, data) => {
        if (err) {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('✅ Server is running!');
        } else {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        }
    });
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
    let currentUsername = null;
    let currentRoomId = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log(`📨 ${msg.type} from ${currentUsername || 'unknown'}`);

            switch (msg.type) {
                case 'register':
                    const roomId = msg.roomId;
                    const username = msg.username;
                    
                    currentUsername = username;
                    currentRoomId = roomId;
                    
                    // Initialize room if needed
                    if (!clients[roomId]) clients[roomId] = {};
                    if (!publicMessages[roomId]) publicMessages[roomId] = [];
                    
                    // ===== UPDATE USER CONNECTION =====
                    // If user already exists with old connection, close it
                    if (clients[roomId][username]) {
                        const oldWs = clients[roomId][username];
                        if (oldWs !== ws && oldWs.readyState === WebSocket.OPEN) {
                            console.log(`🔄 Closing old connection for ${username}`);
                            oldWs.close(1000, 'New connection established');
                        }
                    }
                    
                    // Store new connection
                    clients[roomId][username] = ws;
                    
                    // ===== SEND PUBLIC HISTORY =====
                    const history = publicMessages[roomId].slice(-50);
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: history
                    }));
                    
                    // ===== SEND PRIVATE HISTORY =====
                    const userPrivateMessages = [];
                    for (const [key, messages] of Object.entries(privateMessages)) {
                        if (key.includes(username) || key.startsWith(roomId + '_')) {
                            const relevant = messages.filter(m => 
                                m.sender === username || m.targetUser === username
                            );
                            userPrivateMessages.push(...relevant);
                        }
                    }
                    
                    const uniquePrivate = [];
                    const seenIds = new Set();
                    for (const msg of userPrivateMessages) {
                        if (!seenIds.has(msg.id)) {
                            seenIds.add(msg.id);
                            uniquePrivate.push(msg);
                        }
                    }
                    uniquePrivate.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
                    
                    ws.send(JSON.stringify({
                        type: 'private-history',
                        messages: uniquePrivate.slice(-50)
                    }));
                    
                    // ===== BROADCAST UPDATED USER LIST =====
                    const userList = Object.keys(clients[roomId]);
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: userList
                    });
                    
                    console.log(`✅ ${username} joined/refreshed room ${roomId}`);
                    console.log(`👥 Users: ${userList.join(', ')}`);
                    break;

                case 'message':
                    if (!currentUsername || !currentRoomId) return;
                    
                    const messageData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: currentUsername,
                        message: msg.message,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null
                    };
                    
                    if (messageData.isPrivate && messageData.targetUser) {
                        // ===== 🔒 PRIVATE MESSAGE =====
                        console.log(`🔒 Private from ${messageData.sender} to ${messageData.targetUser}`);
                        
                        const privateKey = currentRoomId + '_' + messageData.sender + '_' + messageData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(messageData);
                        
                        const reverseKey = currentRoomId + '_' + messageData.targetUser + '_' + messageData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(messageData);
                        
                        // ===== SEND TO TARGET (if online) =====
                        if (clients[currentRoomId][messageData.targetUser]) {
                            clients[currentRoomId][messageData.targetUser].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                            console.log(`✅ Sent private to ${messageData.targetUser}`);
                        } else {
                            console.log(`⚠️ ${messageData.targetUser} not online, message stored`);
                        }
                        
                        // Send to sender
                        if (clients[currentRoomId][currentUsername]) {
                            clients[currentRoomId][currentUsername].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
                        // ===== 🌐 PUBLIC MESSAGE =====
                        console.log(`💬 Public from ${messageData.sender}`);
                        publicMessages[currentRoomId].push(messageData);
                        broadcast(currentRoomId, {
                            type: 'message',
                            message: messageData
                        });
                    }
                    break;

                case 'file':
                    if (!currentUsername || !currentRoomId) return;
                    
                    const fileData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: currentUsername,
                        fileName: msg.fileName,
                        fileSize: msg.fileSize,
                        fileType: msg.fileType,
                        fileData: msg.fileData,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null,
                        isFile: true
                    };
                    
                    if (fileData.isPrivate && fileData.targetUser) {
                        console.log(`🔒 Private file from ${fileData.sender} to ${fileData.targetUser}`);
                        
                        const privateKey = currentRoomId + '_' + fileData.sender + '_' + fileData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(fileData);
                        
                        const reverseKey = currentRoomId + '_' + fileData.targetUser + '_' + fileData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(fileData);
                        
                        if (clients[currentRoomId][fileData.targetUser]) {
                            clients[currentRoomId][fileData.targetUser].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                        if (clients[currentRoomId][currentUsername]) {
                            clients[currentRoomId][currentUsername].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                    } else {
                        publicMessages[currentRoomId].push(fileData);
                        broadcast(currentRoomId, {
                            type: 'file',
                            message: fileData
                        });
                    }
                    break;

                case 'typing':
                    if (!currentUsername || !currentRoomId) return;
                    
                    const typingData = {
                        type: 'typing',
                        username: currentUsername,
                        isTyping: msg.isTyping
                    };
                    
                    if (msg.isPrivate && msg.targetUser) {
                        if (clients[currentRoomId][msg.targetUser]) {
                            clients[currentRoomId][msg.targetUser].send(JSON.stringify(typingData));
                        }
                    } else {
                        broadcast(currentRoomId, typingData, [currentUsername]);
                    }
                    break;

                case 'leave':
                    handleDisconnect(currentUsername, currentRoomId);
                    break;

                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
            }
        } catch (e) {
            console.error('Error:', e);
        }
    });

    ws.on('close', () => {
        // Only disconnect if this is still the active connection for this user
        if (currentUsername && currentRoomId) {
            if (clients[currentRoomId] && clients[currentRoomId][currentUsername] === ws) {
                handleDisconnect(currentUsername, currentRoomId);
            }
        }
    });
});

function broadcast(roomId, data, exclude = []) {
    if (!clients[roomId]) return;
    
    const message = JSON.stringify(data);
    Object.keys(clients[roomId]).forEach(username => {
        if (exclude.includes(username)) return;
        const ws = clients[roomId][username];
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(message);
        }
    });
}

function handleDisconnect(username, roomId) {
    if (!username || !roomId) return;
    
    if (clients[roomId]) {
        // Check if this is still the active connection
        if (clients[roomId][username]) {
            delete clients[roomId][username];
            const userList = Object.keys(clients[roomId]);
            broadcast(roomId, {
                type: 'user-left',
                users: userList
            });
            console.log(`👋 ${username} left room ${roomId}`);
            console.log(`👥 Remaining: ${userList.join(', ') || 'none'}`);
        }
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔══════════════════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server                                        ║
    ║   Running on: http://0.0.0.0:${PORT}                            ║
    ║                                                                 ║
    ║   🔒 Users tracked by username (not connection ID)            ║
    ║   🔄 When user refreshes, old connection is replaced           ║
    ║   📨 Messages are delivered to the active connection           ║
    ║   🔒 Private messages: ONLY sender + recipient see them       ║
    ║   📎 File sharing supported                                    ║
    ╚══════════════════════════════════════════════════════════════════╝
    `);
});
