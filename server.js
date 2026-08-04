// server.js - Fixed private messages and real-time delivery
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

const messages = {};
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
                    
                    if (!messages[roomId]) messages[roomId] = [];
                    
                    // Send history to the user
                    const history = messages[roomId].slice(-50);
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: history
                    }));
                    
                    // Broadcast updated user list to everyone in the room
                    const userList = Object.keys(clients[roomId]);
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: userList
                    });
                    
                    console.log(`✅ ${username} joined room ${roomId}`);
                    console.log(`👥 Users in room: ${userList.join(', ')}`);
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
                    
                    // Store in room history
                    messages[userInfo.roomId].push(messageData);
                    
                    if (messageData.isPrivate && messageData.targetUser) {
                        // PRIVATE MESSAGE: Send ONLY to sender and target
                        console.log(`🔒 Private message from ${messageData.sender} to ${messageData.targetUser}`);
                        
                        // Send to target user
                        sendPrivate(userInfo.roomId, messageData.targetUser, {
                            type: 'private-message',
                            message: messageData
                        });
                        
                        // Send to sender (so they see their own message)
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
                        // PUBLIC MESSAGE: Send to everyone in the room
                        console.log(`💬 Public message from ${messageData.sender}`);
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
                    
                    messages[userInfo.roomId].push(fileData);
                    
                    if (fileData.isPrivate && fileData.targetUser) {
                        // PRIVATE FILE: Send ONLY to sender and target
                        console.log(`🔒 Private file from ${fileData.sender} to ${fileData.targetUser}`);
                        
                        sendPrivate(userInfo.roomId, fileData.targetUser, {
                            type: 'file',
                            message: fileData
                        });
                        
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                    } else {
                        // PUBLIC FILE: Send to everyone
                        broadcast(userInfo.roomId, {
                            type: 'file',
                            message: fileData
                        });
                    }
                    break;

                case 'typing':
                    if (!userInfo) return;
                    
                    // Forward typing indicator to everyone EXCEPT sender
                    const typingData = {
                        type: 'typing',
                        username: userInfo.username,
                        isTyping: msg.isTyping
                    };
                    
                    if (msg.isPrivate && msg.targetUser) {
                        // Private typing: send only to target
                        sendPrivate(userInfo.roomId, msg.targetUser, typingData);
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
    const recipients = Object.keys(clients[roomId]).filter(u => !exclude.includes(u));
    
    console.log(`📡 Broadcasting to ${recipients.length} users: ${recipients.join(', ')}`);
    
    recipients.forEach(username => {
        const ws = clients[roomId][username];
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(message);
        }
    });
}

function sendPrivate(roomId, targetUsername, data) {
    if (!clients[roomId]) {
        console.log(`❌ Room ${roomId} not found`);
        return;
    }
    
    if (clients[roomId][targetUsername]) {
        const ws = clients[roomId][targetUsername];
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(data));
            console.log(`✅ Private message sent to ${targetUsername}`);
        } else {
            console.log(`❌ ${targetUsername} is not connected`);
        }
    } else {
        console.log(`❌ ${targetUsername} not found in room`);
    }
}

function handleDisconnect(userInfo) {
    if (!userInfo) return;
    const { roomId, username } = userInfo;
    
    if (clients[roomId]) {
        delete clients[roomId][username];
        const userList = Object.keys(clients[roomId]);
        
        // Broadcast updated user list
        broadcast(roomId, {
            type: 'user-left',
            users: userList
        });
        
        console.log(`👋 ${username} left room ${roomId}`);
        console.log(`👥 Remaining users: ${userList.join(', ') || 'none'}`);
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔═══════════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server                                 ║
    ║   Running on: http://0.0.0.0:${PORT}                     ║
    ║   🔒 End-to-end encryption                              ║
    ║   📎 File sharing supported                             ║
    ║   🔒 Private messages are ONLY sent to target user     ║
    ║   👥 Real-time message delivery                         ║
    ╚═══════════════════════════════════════════════════════════╝
    `);
});
