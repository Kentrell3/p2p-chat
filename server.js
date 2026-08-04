// server.js - Complete Fixed Version
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

// ===== STORAGE =====
const publicMessages = {};
const privateMessages = {};
const groupMessages = {};
const groupMembers = {};
const deletedMessages = {};
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
                case 'register': {
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
                    
                    if (clients[roomId][username]) {
                        const oldWs = clients[roomId][username];
                        if (oldWs !== ws && oldWs.readyState === WebSocket.OPEN) {
                            console.log(`🔄 Closing old connection for ${username}`);
                            oldWs.close(1000, 'New connection established');
                        }
                    }
                    
                    clients[roomId][username] = ws;
                    
                    const userDeleted = deletedMessages[roomId][username] || new Set();
                    
                    // Send public history
                    const publicHistory = publicMessages[roomId]
                        .filter(m => !userDeleted.has(m.id))
                        .slice(-50);
                    ws.send(JSON.stringify({ type: 'history', messages: publicHistory }));
                    
                    // Send private history
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
                    for (const m of userPrivateMessages) {
                        if (!seenIds.has(m.id) && !userDeleted.has(m.id)) {
                            seenIds.add(m.id);
                            uniquePrivate.push(m);
                        }
                    }
                    uniquePrivate.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
                    ws.send(JSON.stringify({ type: 'private-history', messages: uniquePrivate.slice(-50) }));
                    
                    // Send group history
                    for (const [groupId, members] of Object.entries(groupMembers)) {
                        if (members.includes(username)) {
                            const groupMsgs = (groupMessages[groupId] || [])
                                .filter(m => !userDeleted.has(m.id))
                                .slice(-50);
                            ws.send(JSON.stringify({
                                type: 'group-history',
                                groupId: groupId,
                                messages: groupMsgs
                            }));
                        }
                    }
                    
                    const userList = Object.keys(clients[roomId]);
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: userList
                    });
                    
                    console.log(`✅ ${username} joined room ${roomId}`);
                    break;
                }

                case 'create-group': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const groupId = 'group_' + Date.now().toString(36) + Math.random().toString(36).substring(2, 6);
                    const groupName = msg.groupName || 'New Group';
                    const members = msg.members || [];
                    
                    if (!members.includes(currentUsername)) {
                        members.push(currentUsername);
                    }
                    
                    groupMembers[groupId] = members;
                    groupMessages[groupId] = [];
                    
                    console.log(`👥 Group created: ${groupName} (${groupId}) by ${currentUsername}`);
                    
                    for (const member of members) {
                        if (clients[currentRoomId][member]) {
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'group-created',
                                groupId: groupId,
                                groupName: groupName,
                                members: members,
                                createdBy: currentUsername
                            }));
                        }
                    }
                    break;
                }

                case 'group-message': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { groupId: gId, message } = msg;
                    
                    if (!groupMembers[gId] || !groupMembers[gId].includes(currentUsername)) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'You are not a member of this group'
                        }));
                        return;
                    }
                    
                    const groupMsgData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: currentUsername,
                        message: message,
                        timestamp: Date.now(),
                        groupId: gId
                    };
                    
                    if (!groupMessages[gId]) groupMessages[gId] = [];
                    groupMessages[gId].push(groupMsgData);
                    
                    for (const member of groupMembers[gId]) {
                        if (clients[currentRoomId][member]) {
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'group-message',
                                message: groupMsgData
                            }));
                        }
                    }
                    console.log(`💬 Group message in ${gId} from ${currentUsername}`);
                    break;
                }

                case 'group-file': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { groupId: gfId, fileName, fileSize, fileType, fileData: gfData, id: gfMsgId } = msg;
                    
                    if (!groupMembers[gfId] || !groupMembers[gfId].includes(currentUsername)) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'You are not a member of this group'
                        }));
                        return;
                    }
                    
                    const groupFileMsg = {
                        id: gfMsgId || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: currentUsername,
                        fileName: fileName,
                        fileSize: fileSize,
                        fileType: fileType,
                        fileData: gfData,
                        timestamp: Date.now(),
                        groupId: gfId,
                        isFile: true
                    };
                    
                    if (!groupMessages[gfId]) groupMessages[gfId] = [];
                    groupMessages[gfId].push(groupFileMsg);
                    
                    for (const member of groupMembers[gfId]) {
                        if (clients[currentRoomId][member]) {
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'group-file',
                                message: groupFileMsg
                            }));
                        }
                    }
                    console.log(`📎 Group file in ${gfId} from ${currentUsername}`);
                    break;
                }

                case 'add-to-group': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { groupId: agId, newMember } = msg;
                    
                    if (!groupMembers[agId]) {
                        ws.send(JSON.stringify({ type: 'error', message: 'Group not found' }));
                        return;
                    }
                    
                    if (!groupMembers[agId].includes(currentUsername)) {
                        ws.send(JSON.stringify({ type: 'error', message: 'You are not a member of this group' }));
                        return;
                    }
                    
                    if (groupMembers[agId].includes(newMember)) {
                        ws.send(JSON.stringify({ type: 'error', message: `${newMember} is already in the group` }));
                        return;
                    }
                    
                    groupMembers[agId].push(newMember);
                    
                    for (const member of groupMembers[agId]) {
                        if (clients[currentRoomId][member]) {
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'group-member-added',
                                groupId: agId,
                                member: newMember,
                                addedBy: currentUsername,
                                members: groupMembers[agId]
                            }));
                        }
                    }
                    console.log(`➕ ${newMember} added to group ${agId} by ${currentUsername}`);
                    break;
                }

                case 'delete-group-message': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { groupId: dgId, messageId: dMsgId } = msg;
                    
                    if (!groupMembers[dgId] || !groupMembers[dgId].includes(currentUsername)) {
                        ws.send(JSON.stringify({ type: 'error', message: 'You are not a member of this group' }));
                        return;
                    }
                    
                    if (!deletedMessages[currentRoomId][currentUsername]) {
                        deletedMessages[currentRoomId][currentUsername] = new Set();
                    }
                    deletedMessages[currentRoomId][currentUsername].add(dMsgId);
                    
                    for (const member of groupMembers[dgId]) {
                        if (clients[currentRoomId][member]) {
                            if (!deletedMessages[currentRoomId][member]) {
                                deletedMessages[currentRoomId][member] = new Set();
                            }
                            deletedMessages[currentRoomId][member].add(dMsgId);
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'message-deleted',
                                messageId: dMsgId
                            }));
                        }
                    }
                    console.log(`🗑️ Group message ${dMsgId} deleted by ${currentUsername}`);
                    break;
                }

                case 'leave-group': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { groupId: lgId } = msg;
                    
                    if (!groupMembers[lgId]) {
                        ws.send(JSON.stringify({ type: 'error', message: 'Group not found' }));
                        return;
                    }
                    
                    const index = groupMembers[lgId].indexOf(currentUsername);
                    if (index !== -1) {
                        groupMembers[lgId].splice(index, 1);
                    }
                    
                    for (const member of groupMembers[lgId]) {
                        if (clients[currentRoomId][member]) {
                            clients[currentRoomId][member].send(JSON.stringify({
                                type: 'group-member-left',
                                groupId: lgId,
                                member: currentUsername,
                                members: groupMembers[lgId]
                            }));
                        }
                    }
                    console.log(`🚪 ${currentUsername} left group ${lgId}`);
                    break;
                }

                case 'message': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const msgData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: currentUsername,
                        message: msg.message,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null
                    };
                    
                    if (msgData.isPrivate && msgData.targetUser) {
                        console.log(`🔒 Private from ${msgData.sender} to ${msgData.targetUser}`);
                        
                        const privateKey = currentRoomId + '_' + msgData.sender + '_' + msgData.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(msgData);
                        
                        const reverseKey = currentRoomId + '_' + msgData.targetUser + '_' + msgData.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(msgData);
                        
                        if (clients[currentRoomId][msgData.targetUser]) {
                            clients[currentRoomId][msgData.targetUser].send(JSON.stringify({
                                type: 'private-message',
                                message: msgData
                            }));
                        }
                        if (clients[currentRoomId][currentUsername]) {
                            clients[currentRoomId][currentUsername].send(JSON.stringify({
                                type: 'private-message',
                                message: msgData
                            }));
                        }
                    } else {
                        console.log(`💬 Public from ${msgData.sender}`);
                        publicMessages[currentRoomId].push(msgData);
                        broadcast(currentRoomId, {
                            type: 'message',
                            message: msgData
                        });
                    }
                    break;
                }

                case 'file': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const fileMsg = {
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
                    
                    if (fileMsg.isPrivate && fileMsg.targetUser) {
                        console.log(`🔒 Private file from ${fileMsg.sender} to ${fileMsg.targetUser}`);
                        
                        const privateKey = currentRoomId + '_' + fileMsg.sender + '_' + fileMsg.targetUser;
                        if (!privateMessages[privateKey]) privateMessages[privateKey] = [];
                        privateMessages[privateKey].push(fileMsg);
                        
                        const reverseKey = currentRoomId + '_' + fileMsg.targetUser + '_' + fileMsg.sender;
                        if (!privateMessages[reverseKey]) privateMessages[reverseKey] = [];
                        privateMessages[reverseKey].push(fileMsg);
                        
                        if (clients[currentRoomId][fileMsg.targetUser]) {
                            clients[currentRoomId][fileMsg.targetUser].send(JSON.stringify({
                                type: 'file',
                                message: fileMsg
                            }));
                        }
                        if (clients[currentRoomId][currentUsername]) {
                            clients[currentRoomId][currentUsername].send(JSON.stringify({
                                type: 'file',
                                message: fileMsg
                            }));
                        }
                    } else {
                        publicMessages[currentRoomId].push(fileMsg);
                        broadcast(currentRoomId, {
                            type: 'file',
                            message: fileMsg
                        });
                    }
                    break;
                }

                case 'delete-message': {
                    if (!currentUsername || !currentRoomId) return;
                    
                    const { messageId, isPrivate, targetUser } = msg;
                    console.log(`🗑️ ${currentUsername} deleting message ${messageId}`);
                    
                    if (!deletedMessages[currentRoomId]) deletedMessages[currentRoomId] = {};
                    if (!deletedMessages[currentRoomId][currentUsername]) {
                        deletedMessages[currentRoomId][currentUsername] = new Set();
                    }
                    deletedMessages[currentRoomId][currentUsername].add(messageId);
                    
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
                    break;
                }

                case 'clear-chat': {
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
                        
                        const msgsToDelete = [];
                        if (privateMessages[privateKey]) {
                            msgsToDelete.push(...privateMessages[privateKey]);
                        }
                        if (privateMessages[reverseKey]) {
                            msgsToDelete.push(...privateMessages[reverseKey]);
                        }
                        
                        for (const m of msgsToDelete) {
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
                    } else if (chatType === 'group') {
                        const groupId = clearTarget;
                        if (groupMessages[groupId]) {
                            for (const m of groupMessages[groupId]) {
                                deletedMessages[currentRoomId][currentUsername].add(m.id);
                            }
                            ws.send(JSON.stringify({
                                type: 'chat-cleared',
                                chatId: groupId
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
                    break;
                }

                case 'typing': {
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
                    } else if (msg.groupId) {
                        const gMembers = groupMembers[msg.groupId] || [];
                        for (const member of gMembers) {
                            if (member !== currentUsername && clients[currentRoomId][member]) {
                                clients[currentRoomId][member].send(JSON.stringify({
                                    ...typingData,
                                    groupId: msg.groupId
                                }));
                            }
                        }
                    } else {
                        broadcast(currentRoomId, typingData, [currentUsername]);
                    }
                    break;
                }

                case 'leave': {
                    handleDisconnect(currentUsername, currentRoomId);
                    break;
                }

                case 'ping': {
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
                }
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
    ║   👥 Group Chats - Create groups and add members              ║
    ║   💬 Public chats - Everyone in the room                      ║
    ║   🔒 Private chats - 1-on-1 conversations                     ║
    ║   🗑️ Delete individual messages                               ║
    ║   🗑️ Clear chat permanently                                   ║
    ║   📎 File sharing                                              ║
    ╚══════════════════════════════════════════════════════════════════╝
    `);
});
