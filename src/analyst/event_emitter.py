"""Event Emitter Infrastructure - Handles loosely-coupled system events."""

from __future__ import annotations
from typing import Callable, Any

class EventEmitter:
    """Core infra to emit system events without tight cyclic component coupling."""
    def __init__(self):
        self._listeners: dict[str, list[Callable[..., Any]]] = {}

    def on(self, event_name: str, callback: Callable[..., Any]) -> None:
        if event_name not in self._listeners:
            self._listeners[event_name] = []
        self._listeners[event_name].append(callback)

    def off(self, event_name: str, callback: Callable[..., Any]) -> None:
        if event_name in self._listeners:
            try:
                self._listeners[event_name].remove(callback)
            except ValueError:
                pass

    def emit(self, event_name: str, *args, **kwargs) -> None:
        for listener in self._listeners.get(event_name, []):
            try:
                listener(*args, **kwargs)
            except Exception:
                pass

# Global Singleton Emitter for the application
global_emitter = EventEmitter()
