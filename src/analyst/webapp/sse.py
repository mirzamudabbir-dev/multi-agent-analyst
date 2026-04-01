import asyncio
from typing import AsyncGenerator

from analyst.event_emitter import global_emitter

class SSEStreamer:
    """Bridges the synchronous EventEmitter to an async FastAPI SSE stream."""
    
    def __init__(self):
        self.queue = asyncio.Queue()
        # Register the synchronous callback that puts items in the async queue
        global_emitter.on("log", self._on_log_event)
        
    def _on_log_event(self, event_data: dict) -> None:
        """Called by the global emitter synchronously."""
        # Use simple try-put since we can't easily await in the synchronous emitter
        try:
            self.queue.put_nowait(event_data)
        except asyncio.QueueFull:
            pass
            
    async def sse_generator(self) -> AsyncGenerator[str, None]:
        """Async generator that yields SSE formatted strings."""
        try:
            while True:
                # Wait for the next event in the queue
                event_data = await self.queue.get()
                
                # Format as Server-Sent Event
                import json
                data_str = json.dumps(event_data)
                yield f"data: {data_str}\n\n"
        except asyncio.CancelledError:
            # Client disconnected
            pass
        finally:
            # UNREGISTER the listener to prevent memory leaks!
            global_emitter.off("log", self._on_log_event)
