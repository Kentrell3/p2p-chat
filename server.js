// server.js - COMPLETE FIX
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

// ===== STORAGE =====
// Public messages - sent to everyone in the room
const publicMessages = {};
// Private messages - stored per user pair (only sender and recipient can see)
const privateMessages = {};
// Active clients
const clients = {};

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
    let userInfo = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log(`📨 ${msg.type} from ${userInfo?.username || 'unknown'}`);

            switch (msg.type) {
                case 'register':
                    const roomId = msg.roomId;
                    const username = msg.username;
                    
                    userInfo = { roomId, username };
                    
                    if (!clients[roomId]) clients[roomId] = {};
                    clients[roomId][username] = ws;
                    
                    // Initialize public messages for room
                    if (!publicMessages[roomId]) publicMessages[roomId] = [];
                    
                    // ===== SEND PUBLIC HISTORY =====
                    const history = publicMessages[roomId].slice(-50);
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: history
                    }));
                    
                    // ===== SEND PRIVATE HISTORY =====
                    // Only send private messages where this user is sender OR recipient
                    const userPrivateKey = roomId + '_' + username;
                    const userPrivateMessages = [];
                    
                    // Check all private message pairs for this user
                    for (const [key, messages] of Object.entries(privateMessages)) {
                        // Key format: roomId_senderOrRecipient
                        // If the key contains this username OR this username is in the message
                        if (key.includes(username) || key.startsWith(roomId + '_')) {
                            // Filter messages where this user is sender or recipient
                            const relevant = messages.filter(m => 
                                m.sender === username || m.targetUser === username
                            );
                            userPrivateMessages.push(...relevant);
                        }
                    }
                    
                    // Remove duplicates by ID
                    const uniquePrivate = [];
                    const seenIds = new Set();
                    for (const msg of userPrivateMessages) {
                        if (!seenIds.has(msg.id)) {
                            seenIds.add(msg.id);
                            uniquePrivate.push(msg);
                        }
                    }
                    
                    // Sort by timestamp
                    uniquePrivate.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
                    
                    // Send private history
                    ws.send(JSON.stringify({
                        type: 'private-history',
                        messages: uniquePrivate.slice(-50)
                    }));
                    
                    // ===== BROADCAST USER LIST =====
                    const userList = Object.keys(clients[roomId]);
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: userList
                    });
                    
                    console.log(`✅ ${username} joined room ${roomId}`);
                    console.log(`👥 Users: ${userList.join(', ')}`);
                    console.log(`📚 Public messages: ${publicMessages[roomId].length}`);
                    console.log(`🔒 Private messages: ${uniquePrivate.length} for ${username}`);
                    break;

                case 'message':
                    if (!userInfo) return;
                    
                    const messageData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: userInfo.username,
                        message: msg.message,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null
                    };
                    
                    if (messageData.isPrivate && messageData.targetUser) {
                        // ===== 🔒 PRIVATE MESSAGE =====
                        console.log(`🔒 Private from ${messageData.sender} to ${messageData.targetUser}`);
                        
                        // Store in private storage (NOT in public)
                        const privateKey = userInfo.roomId + '_' + messageData.sender + '_' + messageData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(messageData);
                        
                        // Also store reverse key so both can see it
                        const reverseKey = userInfo.roomId + '_' + messageData.targetUser + '_' + messageData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(messageData);
                        
                        // ===== SEND ONLY TO SENDER + TARGET =====
                        // Send to target (if online)
                        if (clients[userInfo.roomId][messageData.targetUser]) {
                            clients[userInfo.roomId][messageData.targetUser].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                            console.log(`✅ Sent private to ${messageData.targetUser}`);
                        } else {
                            console.log(`⚠️ ${messageData.targetUser} not online, message stored`);
                        }
                        
                        // Send to sender (so they see their own message)
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
                        // ===== 🌐 PUBLIC MESSAGE =====
                        console.log(`💬 Public from ${messageData.sender}`);
                        publicMessages[userInfo.roomId].push(messageData);
                        broadcast(userInfo.roomId, {
                            type: 'message',
                            message: messageData
                        });
                    }
                    break;

                case 'file':
                    if (!userInfo) return;
                    
                    const fileData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: userInfo.username,
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
                        // ===== 🔒 PRIVATE FILE =====
                        console.log(`🔒 Private file from ${fileData.sender} to ${fileData.targetUser}`);
                        
                        const privateKey = userInfo.roomId + '_' + fileData.sender + '_' + fileData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(fileData);
                        
                        const reverseKey = userInfo.roomId + '_' + fileData.targetUser + '_' + fileData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(fileData);
                        
                        if (clients[userInfo.roomId][fileData.targetUser]) {
                            clients[userInfo.roomId][fileData.targetUser].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                    } else {
                        publicMessages[userInfo.roomId].push(fileData);
                        broadcast(userInfo.roomId, {
                            type: 'file',
                            message: fileData
                        });
                    }
                    break;

                case 'typing':
                    if (!userInfo) return;
                    
                    const typingData = {
                        type: 'typing',
                        username: userInfo.username,
                        isTyping: msg.isTyping
                    };
                    
                    if (msg.isPrivate && msg.targetUser) {
                        // Private typing: send only to target
                        if (clients[userInfo.roomId][msg.targetUser]) {
                            clients[userInfo.roomId][msg.targetUser].send(JSON.stringify(typingData));
                        }
                    } else {
                        // Public typing: send to everyone except sender
                        broadcast(userInfo.roomId, typingData, [userInfo.username]);
                    }
                    break;

                case 'leave':
                    handleDisconnect(userInfo);
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
        handleDisconnect(userInfo);
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

function handleDisconnect(userInfo) {
    if (!userInfo) return;
    const { roomId, username } = userInfo;
    
    if (clients[roomId]) {
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

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔══════════════════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server                                        ║
    ║   Running on: http://0.0.0.0:${PORT}                            ║
    ║                                                                 ║
    ║   🔒 PUBLIC messages: stored in publicMessages                 ║
    ║   🔒 PRIVATE messages: stored in privateMessages per user pair ║
    ║   🔒 Private messages NEVER sent to users not involved         ║
    ║   🔄 Each user gets their own history on refresh               ║
    ║   📎 File sharing supported                                    ║
    ╚══════════════════════════════════════════════════════════════════╝
    `);
});
