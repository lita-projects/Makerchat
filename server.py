import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
import os
from aiohttp import web
from aiohttp_middlewares import cors_middleware
import aiofiles
import uuid
from dataclasses import dataclass, asdict
from collections import defaultdict
from typing import Dict, Set, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class VoiceParticipant:
    username: str
    avatar: str
    client_id: str

class VoiceManager:
    def __init__(self):
        self.participants: Dict[str, VoiceParticipant] = {}
    
    def add_participant(self, username: str, avatar: str, client_id: str):
        self.participants[client_id] = VoiceParticipant(username, avatar, client_id)
    
    def remove_participant(self, client_id: str):
        if client_id in self.participants:
            del self.participants[client_id]
    
    def get_participants(self):
        return [asdict(p) for p in self.participants.values()]

class FileStorageManager:
    def __init__(self):
        self.base_path = Path(os.environ.get('STORAGE_PATH', 'data'))
        self.messages_file = self.base_path / 'messages.json'
        self.users_file = self.base_path / 'users.json'
        self._init_storage()

    def _init_storage(self):
        self.base_path.mkdir(exist_ok=True)
        
        if not self.messages_file.exists():
            self._write_json(self.messages_file, [])
        if not self.users_file.exists():
            self._write_json(self.users_file, {})

    def _read_json(self, file_path: Path) -> dict:
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {} if file_path == self.users_file else []
        except json.JSONDecodeError:
            logger.error(f"Error reading {file_path}. Initializing as empty.")
            return {} if file_path == self.users_file else []

    def _write_json(self, file_path: Path, data: dict):
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

    async def _async_read_json(self, file_path: Path) -> dict:
        try:
            async with aiofiles.open(file_path, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except FileNotFoundError:
            return {} if file_path == self.users_file else []
        except json.JSONDecodeError:
            logger.error(f"Error reading {file_path}. Initializing as empty.")
            return {} if file_path == self.users_file else []

    async def _async_write_json(self, file_path: Path, data: dict):
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(data, indent=2))

    async def get_user_profile(self, ip_address: str) -> dict:
        users = await self._async_read_json(self.users_file)
        return users.get(ip_address)

    async def save_user_profile(self, ip_address: str, username: str, avatar: str):
        users = await self._async_read_json(self.users_file)
        users[ip_address] = {
            'username': username,
            'avatar': avatar,
            'last_seen': datetime.now().isoformat()
        }
        await self._async_write_json(self.users_file, users)

    async def save_message(self, username: str, avatar: str, content: str, ip_address: str) -> str:
        messages = await self._async_read_json(self.messages_file)
        timestamp = datetime.now().isoformat()
        
        message = {
            'id': str(uuid.uuid4()),
            'username': username,
            'avatar': avatar,
            'content': content,
            'timestamp': timestamp,
            'ip_address': ip_address
        }
        
        messages.append(message)
        if len(messages) > 1000:
            messages = messages[-1000:]
            
        await self._async_write_json(self.messages_file, messages)
        return timestamp

    async def get_recent_messages(self, limit: int = 50, current_ip: str = None) -> list:
        messages = await self._async_read_json(self.messages_file)
        recent_messages = messages[-limit:]
        
        return [{
            "type": "message",
            "username": msg['username'],
            "avatar": msg['avatar'],
            "content": msg['content'],
            "timestamp": msg['timestamp'],
            "is_own_message": str(msg['ip_address']) == str(current_ip),
            "ip_address": msg['ip_address']
        } for msg in recent_messages]

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.db = FileStorageManager()
        self.voice_manager = VoiceManager()

    async def connect(self, websocket: web.WebSocketResponse, client_id: str, ip_address: str):
        try:
            profile = await self.db.get_user_profile(ip_address)
            
            if profile:
                username = profile['username']
                avatar = profile['avatar']
            else:
                username = f"User-{client_id[:8]}"
                avatar = f"https://api.dicebear.com/6.x/avataaars/svg?seed={client_id}"
                try:
                    await self.db.save_user_profile(ip_address, username, avatar)
                except Exception as e:
                    logger.error(f"Error saving new user profile: {e}")

            self.active_connections[client_id] = {
                'websocket': websocket,
                'username': username,
                'avatar': avatar,
                'ip_address': ip_address,
                'last_pong': datetime.now()
            }
            logger.info(f"Client {client_id} connected from {ip_address}")
        except Exception as e:
            logger.error(f"Error in connect: {e}")
            raise

    async def disconnect(self, client_id: str, broadcast_message=True):
        if client_id in self.active_connections:
            if broadcast_message:
                disconnected_user = self.active_connections[client_id]['username']
                await self._broadcast_to_others({
                    'type': 'user_left',
                    'username': disconnected_user
                }, client_id)
            
            del self.active_connections[client_id]
            logger.info(f"Client {client_id} disconnected")

    async def update_profile(self, client_id: str, new_username: str, new_avatar: str):
        if client_id in self.active_connections:
            connection = self.active_connections[client_id]
            old_username = connection['username']
            
            connection['username'] = new_username
            connection['avatar'] = new_avatar
            
            try:
                await self.db.save_user_profile(
                    connection['ip_address'],
                    new_username,
                    new_avatar
                )
            except Exception as e:
                logger.error(f"Error updating profile in database: {e}")
                return
            
            await self._broadcast_to_others({
                'type': 'user_updated',
                'old_username': old_username,
                'new_username': new_username
            }, client_id)
            
            await connection['websocket'].send_json({
                'type': 'profile_updated',
                'username': new_username,
                'avatar': new_avatar
            })
            
            logger.info(f"Client {client_id} updated profile: {new_username}")

    async def handle_voice_message(self, client_id: str, message_type: str, data: dict):
        if message_type == 'voice_join':
            self.voice_manager.add_participant(
                data['username'],
                data['avatar'],
                client_id
            )
            await self.broadcast({
                'type': 'voice_participants',
                'participants': self.voice_manager.get_participants()
            })
            
            # Send ICE candidates and SDP offers to new participant
            if len(self.voice_manager.participants) > 1:
                await self._broadcast_to_others({
                    'type': 'voice_peer_joined',
                    'username': data['username'],
                    'client_id': client_id
                }, client_id)
        
        elif message_type == 'voice_leave':
            self.voice_manager.remove_participant(client_id)
            await self.broadcast({
                'type': 'voice_participants',
                'participants': self.voice_manager.get_participants()
            })
            
            await self._broadcast_to_others({
                'type': 'voice_peer_left',
                'client_id': client_id
            }, client_id)

    async def handle_webrtc_message(self, client_id: str, data: dict):
        target_client_id = data.get('target')
        if target_client_id in self.active_connections:
            try:
                await self.active_connections[target_client_id]['websocket'].send_json({
                    'type': data['type'],
                    'sender': client_id,
                    'sdp': data.get('sdp'),
                    'candidate': data.get('candidate')
                })
            except Exception as e:
                logger.error(f"Error sending WebRTC message: {e}")

    async def _broadcast_to_others(self, message: dict, sender_id: str = None):
        for client_id, client in list(self.active_connections.items()):
            if client_id != sender_id:
                try:
                    await client['websocket'].send_json(message)
                except Exception as e:
                    logger.error(f"Failed to send to client {client_id}: {e}")
                    await self.disconnect(client_id, broadcast_message=False)

    async def broadcast(self, message: dict, sender_id: str = None):
        sender_connection = self.active_connections.get(sender_id, {})
        sender_ip = sender_connection.get('ip_address')
        
        for client_id, client in list(self.active_connections.items()):
            try:
                message_copy = message.copy()
                message_copy['is_own_message'] = (client.get('ip_address') == sender_ip)
                await client['websocket'].send_json(message_copy)
            except Exception as e:
                logger.error(f"Failed to send to client {client_id}: {e}")
                await self.disconnect(client_id, broadcast_message=False)

    async def handle_ping(self, client_id: str):
        if client_id in self.active_connections:
            self.active_connections[client_id]['last_pong'] = datetime.now()

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    
    client_id = request.match_info['client_id']
    manager = request.app['connection_manager']
    
    try:
        real_ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or \
                 request.headers.get('X-Real-IP', '') or \
                 request.remote or \
                 '127.0.0.1'
                 
        await manager.connect(ws, client_id, real_ip)
        
        recent_messages = await manager.db.get_recent_messages(current_ip=real_ip)
        if recent_messages:
            for msg in recent_messages:
                await ws.send_json(msg)
        
        # Send current voice participants list
        await ws.send_json({
            'type': 'voice_participants',
            'participants': manager.voice_manager.get_participants()
        })
        
        await manager._broadcast_to_others({
            'type': 'user_joined',
            'username': manager.active_connections[client_id]['username']
        }, client_id)

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    if data['type'] == 'ping':
                        await manager.handle_ping(client_id)
                        continue
                    
                    if data['type'] == 'update_profile':
                        await manager.update_profile(
                            client_id,
                            data['username'],
                            data['avatar']
                        )
                        continue
                    
                    if data['type'] in ['voice_join', 'voice_leave']:
                        await manager.handle_voice_message(client_id, data['type'], data)
                        continue
                    
                    if data['type'] in ['offer', 'answer', 'ice-candidate']:
                        await manager.handle_webrtc_message(client_id, data)
                        continue
                        
                    if data['type'] == 'message':
                        content = data.get('content', '').strip()
                        if not content:
                            continue

                        timestamp = await manager.db.save_message(
                            manager.active_connections[client_id]['username'],
                            manager.active_connections[client_id]['avatar'],
                            content,
                            real_ip
                        )
                        
                        await manager.broadcast({
                            'type': 'message',
                            'username': manager.active_connections[client_id]['username'],
                            'avatar': manager.active_connections[client_id]['avatar'],
                            'content': content,
                            'timestamp': timestamp
                        }, client_id)
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client {client_id}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f'WebSocket error: {ws.exception()}')
                
    except Exception as e:
        logger.error(f"WebSocket handler error: {e}")
    finally:
        await manager.disconnect(client_id)
        manager.voice_manager.remove_participant(client_id)
        
    return ws

async def handle_index(request):
    index_path = Path(__file__).parent / 'index.html'
    if not index_path.exists():
        raise web.HTTPNotFound(text="index.html not found")
    return web.FileResponse(index_path)

async def init_app():
    app = web.Application(middlewares=[
        cors_middleware(
            origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ])
    
    app['forwarded_allow_ips'] = '*'
    
    storage_path = Path(os.environ.get('STORAGE_PATH', 'data'))
    storage_path.mkdir(exist_ok=True)
    
    app['connection_manager'] = ConnectionManager()
    app['voice_manager'] = VoiceManager()
    
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws/{client_id}', websocket_handler)
    app.router.add_post('/voice/offer', handle_offer)
    app.router.add_post('/voice/answer', handle_answer)
    app.router.add_post('/voice/ice-candidate', handle_ice_candidate)
    
    return app

class VoiceManager:
    def __init__(self):
        self.voice_rooms = defaultdict(set)
        self.client_rooms = {}
        self.voice_states = {}

    def join_voice(self, client_id: str, room_id: str = "default"):
        self.voice_rooms[room_id].add(client_id)
        self.client_rooms[client_id] = room_id
        self.voice_states[client_id] = {
            "muted": False,
            "deafened": False,
            "speaking": False
        }

    def leave_voice(self, client_id: str):
        if client_id in self.client_rooms:
            room_id = self.client_rooms[client_id]
            self.voice_rooms[room_id].discard(client_id)
            if not self.voice_rooms[room_id]:
                del self.voice_rooms[room_id]
            del self.client_rooms[client_id]
            if client_id in self.voice_states:
                del self.voice_states[client_id]

    def get_room_participants(self, room_id: str) -> Set[str]:
        return self.voice_rooms.get(room_id, set())

    def get_client_room(self, client_id: str) -> Optional[str]:
        return self.client_rooms.get(client_id)

    def update_voice_state(self, client_id: str, state: dict):
        if client_id in self.voice_states:
            self.voice_states[client_id].update(state)

    def get_voice_state(self, client_id: str) -> dict:
        return self.voice_states.get(client_id, {})

async def handle_offer(request):
    data = await request.json()
    manager = request.app['connection_manager']
    voice_manager = request.app['voice_manager']
    
    from_client = data['from']
    to_client = data['to']
    offer = data['offer']
    room_id = voice_manager.get_client_room(from_client)
    
    if not room_id or to_client not in voice_manager.get_room_participants(room_id):
        raise web.HTTPBadRequest(text="Invalid voice session")
    
    try:
        await manager.send_to_client(to_client, {
            'type': 'voice_offer',
            'from': from_client,
            'offer': offer
        })
        return web.Response(text="Offer forwarded")
    except Exception as e:
        logger.error(f"Error handling offer: {e}")
        raise web.HTTPInternalServerError(text="Failed to forward offer")

async def handle_answer(request):
    data = await request.json()
    manager = request.app['connection_manager']
    voice_manager = request.app['voice_manager']
    
    from_client = data['from']
    to_client = data['to']
    answer = data['answer']
    room_id = voice_manager.get_client_room(from_client)
    
    if not room_id or to_client not in voice_manager.get_room_participants(room_id):
        raise web.HTTPBadRequest(text="Invalid voice session")
    
    try:
        await manager.send_to_client(to_client, {
            'type': 'voice_answer',
            'from': from_client,
            'answer': answer
        })
        return web.Response(text="Answer forwarded")
    except Exception as e:
        logger.error(f"Error handling answer: {e}")
        raise web.HTTPInternalServerError(text="Failed to forward answer")

async def handle_ice_candidate(request):
    data = await request.json()
    manager = request.app['connection_manager']
    voice_manager = request.app['voice_manager']
    
    from_client = data['from']
    to_client = data['to']
    candidate = data['candidate']
    room_id = voice_manager.get_client_room(from_client)
    
    if not room_id or to_client not in voice_manager.get_room_participants(room_id):
        raise web.HTTPBadRequest(text="Invalid voice session")
    
    try:
        await manager.send_to_client(to_client, {
            'type': 'ice_candidate',
            'from': from_client,
            'candidate': candidate
        })
        return web.Response(text="ICE candidate forwarded")
    except Exception as e:
        logger.error(f"Error handling ICE candidate: {e}")
        raise web.HTTPInternalServerError(text="Failed to forward ICE candidate")

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.db = FileStorageManager()
        self.voice_manager = VoiceManager()

    async def connect(self, websocket: web.WebSocketResponse, client_id: str, ip_address: str):
        try:
            profile = await self.db.get_user_profile(ip_address)
            
            if profile:
                username = profile['username']
                avatar = profile['avatar']
            else:
                username = f"User-{client_id[:8]}"
                avatar = f"https://api.dicebear.com/6.x/avataaars/svg?seed={client_id}"
                await self.db.save_user_profile(ip_address, username, avatar)

            self.active_connections[client_id] = {
                'websocket': websocket,
                'username': username,
                'avatar': avatar,
                'ip_address': ip_address,
                'last_pong': datetime.now()
            }
            
            logger.info(f"Client {client_id} connected from {ip_address}")

        except Exception as e:
            logger.error(f"Error in connect: {e}")
            raise

    async def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            self.voice_manager.leave_voice(client_id)
            
            # Notify others in the same voice room
            room_id = self.voice_manager.get_client_room(client_id)
            if room_id:
                await self.broadcast_to_room(room_id, {
                    'type': 'voice_peer_disconnected',
                    'client_id': client_id
                }, exclude_client=client_id)
            
            disconnected_user = self.active_connections[client_id]['username']
            await self._broadcast_to_others({
                'type': 'user_left',
                'username': disconnected_user
            }, client_id)
            
            del self.active_connections[client_id]
            logger.info(f"Client {client_id} disconnected")

    async def send_to_client(self, client_id: str, message: dict):
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id]['websocket'].send_json(message)
            except Exception as e:
                logger.error(f"Error sending to client {client_id}: {e}")
                await self.disconnect(client_id)

    async def broadcast_to_room(self, room_id: str, message: dict, exclude_client: str = None):
        participants = self.voice_manager.get_room_participants(room_id)
        for client_id in participants:
            if client_id != exclude_client:
                await self.send_to_client(client_id, message)

    async def handle_voice_message(self, client_id: str, message_type: str, data: dict):
        if message_type == 'voice_join':
            room_id = data.get('room_id', 'default')
            self.voice_manager.join_voice(client_id, room_id)
            
            # Notify existing participants
            participants = self.voice_manager.get_room_participants(room_id)
            for participant_id in participants:
                if participant_id != client_id:
                    await self.send_to_client(participant_id, {
                        'type': 'voice_peer_joined',
                        'client_id': client_id,
                        'username': self.active_connections[client_id]['username'],
                        'avatar': self.active_connections[client_id]['avatar']
                    })
            
            # Send list of existing participants to new user
            await self.send_to_client(client_id, {
                'type': 'voice_participants',
                'participants': [
                    {
                        'client_id': p_id,
                        'username': self.active_connections[p_id]['username'],
                        'avatar': self.active_connections[p_id]['avatar'],
                        'state': self.voice_manager.get_voice_state(p_id)
                    }
                    for p_id in participants if p_id != client_id
                ]
            })
            
        elif message_type == 'voice_leave':
            room_id = self.voice_manager.get_client_room(client_id)
            if room_id:
                self.voice_manager.leave_voice(client_id)
                await self.broadcast_to_room(room_id, {
                    'type': 'voice_peer_left',
                    'client_id': client_id
                }, exclude_client=client_id)
        
        elif message_type == 'voice_state_update':
            self.voice_manager.update_voice_state(client_id, data.get('state', {}))
            room_id = self.voice_manager.get_client_room(client_id)
            if room_id:
                await self.broadcast_to_room(room_id, {
                    'type': 'voice_state_update',
                    'client_id': client_id,
                    'state': self.voice_manager.get_voice_state(client_id)
                }, exclude_client=client_id)

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    
    client_id = request.match_info['client_id']
    manager = request.app['connection_manager']
    
    try:
        real_ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or \
                  request.headers.get('X-Real-IP', '') or \
                  request.remote or \
                  '127.0.0.1'
                  
        await manager.connect(ws, client_id, real_ip)
        
        recent_messages = await manager.db.get_recent_messages(current_ip=real_ip)
        if recent_messages:
            for msg in recent_messages:
                await ws.send_json(msg)
        
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    if data['type'] in ['voice_join', 'voice_leave', 'voice_state_update']:
                        await manager.handle_voice_message(client_id, data['type'], data)
                        continue
                    
                    # Handle other message types (existing chat functionality)
                    await handle_message(manager, client_id, data, real_ip)
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client {client_id}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f'WebSocket error: {ws.exception()}')
                
    except Exception as e:
        logger.error(f"WebSocket handler error: {e}")
    finally:
        await manager.disconnect(client_id)
        
    return ws

async def handle_message(manager: ConnectionManager, client_id: str, data: dict, real_ip: str):
    if data['type'] == 'ping':
        await manager.handle_ping(client_id)
    
    elif data['type'] == 'update_profile':
        await manager.update_profile(
            client_id,
            data['username'],
            data['avatar']
        )
    
    elif data['type'] == 'message':
        content = data.get('content', '').strip()
        if not content:
            return

        timestamp = await manager.db.save_message(
            manager.active_connections[client_id]['username'],
            manager.active_connections[client_id]['avatar'],
            content,
            real_ip
        )
        
        await manager.broadcast({
            'type': 'message',
            'username': manager.active_connections[client_id]['username'],
            'avatar': manager.active_connections[client_id]['avatar'],
            'content': content,
            'timestamp': timestamp
        }, client_id)

def main():
    port = int(os.environ.get('PORT', 8001))
    
    storage_path = Path(os.environ.get('STORAGE_PATH', 'data'))
    storage_path.mkdir(exist_ok=True)
    
    app = asyncio.run(init_app())
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()
