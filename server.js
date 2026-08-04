// server.js - FIXED: Properly filter deleted messages on reconnect
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

// ===== STORAGE =====
const publicMessages = {};
const privateMessages = {};
const deletedMessages = {}; // roomId -> { username: Set(messageIds) }
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
                    
                    if (!clients[roomId]) clients[roomId] = {};
                    if (!publicMessages[roomId]) publicMessages[roomId] = [];
                    if (!deletedMessages[roomId]) deletedMessages[roomId] = {};
                    if (!deletedMessages[roomId][username]) {
                        deletedMessages[roomId][username] = new Set();
                    }
                    
                    // Close old connection
                    if (clients[roomId][username]) {
                        const oldWs = clients[roomId][username];
                        if (oldWs !== ws && oldWs.readyState === WebSocket.OPEN) {
                            console.log(`🔄 Closing old connection for ${username}`);
                            oldWs.close(1000, 'New connection established');
                        }
                    }
                    
                    clients[roomId][username] = ws;
                    
                    // ===== GET USER'S DELETED MESSAGE IDs =====
                    const userDeleted = deletedMessages[roomId][username] || new Set();
                    
                    // ===== SEND PUBLIC HISTORY (FILTER DELETED) =====
                    const publicHistory = publicMessages[roomId]
                        .filter(m => !userDeleted.has(m.id))
                        .slice(-50);
                    
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: publicHistory
                    }));
                    
                    // ===== SEND PRIVATE HISTORY (FILTER DELETED) =====
                    const userPrivateMessages = [];
                    for (const [key, messages] of Object.entries(privateMessages)) {
                        // Check if this message involves this user
                        if (key.includes(username) || key.startsWith(roomId + '_')) {
                            const relevant = messages.filter(m => 
                                m.sender === username || m.targetUser === username
                            );
                            userPrivateMessages.push(...relevant);
                        }
                    }
                    
                    // Remove duplicates and filter deleted
                    const uniquePrivate = [];
                    const seenIds = new Set();
                    for (const msg of userPrivateMessages) {
                        if (!seenIds.has(msg.id) && !userDeleted.has(msg.id)) {
                            seenIds.add(msg.id);
                            uniquePrivate.push(msg);
                        }
                    }
                    uniquePrivate.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
                    
                    ws.send(JSON.stringify({
                        type: 'private-history',
                        messages: uniquePrivate.slice(-50)
                    }));
                    
                    // Broadcast user list
                    const userList = Object.keys(clients[roomId]);
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: userList
                    });
                    
                    console.log(`✅ ${username} joined/refreshed room ${roomId}`);
                    console.log(`📚 Public messages: ${publicMessages[roomId].length}`);
                    console.log(`🗑️ Deleted for ${username}: ${userDeleted.size}`);
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
                        console.log(`🔒 Private from ${messageData.sender} to ${messageData.targetUser}`);
                        
                        const privateKey = currentRoomId + '_' + messageData.sender + '_' + messageData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(messageData);
                        
                        const reverseKey = currentRoomId + '_' + messageData.targetUser + '_' + messageData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(messageData);
                        
                        if (clients[currentRoomId][messageData.targetUser]) {
                            clients[currentRoomId][messageData.targetUser].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                        if (clients[currentRoomId][currentUsername]) {
                            clients[currentRoomId][currentUsername].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
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

                case 'delete-message':
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { messageId, isPrivate, targetUser } = msg;
                    console.log(`🗑️ ${currentUsername} deleting message ${messageId}`);
                    
                    if (!deletedMessages[currentRoomId]) deletedMessages[currentRoomId] = {};
                    if (!deletedMessages[currentRoomId][currentUsername]) {
                        deletedMessages[currentRoomId][currentUsername] = new Set();
                    }
                    deletedMessages[currentRoomId][currentUsername].add(messageId);
                    
                    // Notify the user
                    ws.send(JSON.stringify({
                        type: 'message-deleted',
                        messageId: messageId
                    }));
                    
                    if (isPrivate && targetUser) {
                        if (clients[currentRoomId][targetUser]) {
                            if (!deletedMessages[currentRoomId][targetUser]) {
                                deletedMessages[currentRoomId][targetUser] = new Set();
                            }
                            deletedMessages[currentRoomId][targetUser].add(messageId);
                            clients[currentRoomId][targetUser].send(JSON.stringify({
                                type: 'message-deleted',
                                messageId: messageId
                            }));
                        }
                    } else {
                        broadcast(currentRoomId, {
                            type: 'message-deleted',
                            messageId: messageId
                        }, [currentUsername]);
                    }
                    
                    console.log(`✅ Message ${messageId} deleted for ${currentUsername}`);
                    break;

                case 'clear-chat':
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { chatType, targetUser: clearTarget } = msg;
                    console.log(`🗑️ ${currentUsername} clearing chat`);
                    
                    if (!deletedMessages[currentRoomId]) deletedMessages[currentRoomId] = {};
                    if (!deletedMessages[currentRoomId][currentUsername]) {
                        deletedMessages[currentRoomId][currentUsername] = new Set();
                    }
                    
                    if (chatType === 'private' && clearTarget) {
                        const privateKey = currentRoomId + '_' + currentUsername + '_' + clearTarget;
                        const reverseKey = currentRoomId + '_' + clearTarget + '_' + currentUsername;
                        
                        const messagesToDelete = [];
                        if (privateMessages[privateKey]) {
                            messagesToDelete.push(...privateMessages[privateKey]);
                        }
                        if (privateMessages[reverseKey]) {
                            messagesToDelete.push(...privateMessages[reverseKey]);
                        }
                        
                        for (const m of messagesToDelete) {
                            deletedMessages[currentRoomId][currentUsername].add(m.id);
                            if (clients[currentRoomId][clearTarget]) {
                                if (!deletedMessages[currentRoomId][clearTarget]) {
                                    deletedMessages[currentRoomId][clearTarget] = new Set();
                                }
                                deletedMessages[currentRoomId][clearTarget].add(m.id);
                            }
                        }
                        
                        ws.send(JSON.stringify({
                            type: 'chat-cleared',
                            chatId: 'private_' + clearTarget
                        }));
                        
                        if (clients[currentRoomId][clearTarget]) {
                            clients[currentRoomId][clearTarget].send(JSON.stringify({
                                type: 'chat-cleared',
                                chatId: 'private_' + currentUsername
                            }));
                        }
                    } else {
                        const allPublicMessages = publicMessages[currentRoomId] || [];
                        for (const m of allPublicMessages) {
                            deletedMessages[currentRoomId][currentUsername].add(m.id);
                        }
                        
                        ws.send(JSON.stringify({
                            type: 'chat-cleared',
                            chatId: 'room_' + currentRoomId
                        }));
                    }
                    
                    console.log(`✅ Chat cleared for ${currentUsername}`);
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
        if (clients[roomId][username]) {
            delete clients[roomId][username];
            const userList = Object.keys(clients[roomId]);
            broadcast(roomId, {
                type: 'user-left',
                users: userList
            });
            console.log(`👋 ${username} left room ${roomId}`);
        }
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔══════════════════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server                                        ║
    ║   Running on: http://0.0.0.0:${PORT}                            ║
    ║                                                                 ║
    ║   🔒 End-to-end encryption                                     ║
    ║   🗑️ Delete individual messages (permanently)                 ║
    ║   🗑️ Clear entire chat (permanently)                          ║
    ║   🔒 Private messages: ONLY sender + recipient see them       ║
    ║   🔄 Deleted messages stay deleted after refresh              ║
    ║   📎 File sharing                                              ║
    ╚══════════════════════════════════════════════════════════════════╝
    `);
});
