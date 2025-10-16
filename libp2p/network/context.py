"""
Network context utilities for py-libp2p.

This module provides context utilities similar to Go libp2p's network context,
including support for allowing limited connections (like Circuit Relay v2).
"""

from contextvars import ContextVar
from typing import Any

# Context variables for network options
_allow_limited_conn: ContextVar[str | None] = ContextVar(
    "allow_limited_conn", default=None
)
_no_dial: ContextVar[str | None] = ContextVar("no_dial", default=None)
_force_direct_dial: ContextVar[str | None] = ContextVar(
    "force_direct_dial", default=None
)


def with_allow_limited_conn(reason: str) -> dict[str, Any]:
    """
    Create a context option that instructs the network that it is acceptable
    to use a limited connection when opening a new stream.

    This is the Python equivalent of Go libp2p's network.WithAllowLimitedConn.

    Args:
        reason: Reason for allowing limited connections (for logging/debugging)

    Returns:
        Context dictionary that can be passed to network operations

    """
    return {"allow_limited_conn": reason}


def with_no_dial(reason: str) -> dict[str, Any]:
    """
    Create a context option that instructs the network not to dial new connections.

    Args:
        reason: Reason for not dialing (for logging/debugging)

    Returns:
        Context dictionary that can be passed to network operations

    """
    return {"no_dial": reason}


def with_force_direct_dial(reason: str) -> dict[str, Any]:
    """
    Create a context option that instructs the network to force a direct connection.

    Args:
        reason: Reason for forcing direct dial (for logging/debugging)

    Returns:
        Context dictionary that can be passed to network operations

    """
    return {"force_direct_dial": reason}


def get_allow_limited_conn(
    context: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    """
    Check if the allow limited connection option is set in the context.

    Args:
        context: Optional context dictionary

    Returns:
        Tuple of (is_allowed, reason) where is_allowed is True if limited connections
        are allowed, and reason is the reason string if provided

    """
    if context and "allow_limited_conn" in context:
        return True, context["allow_limited_conn"]

    # Check context variable as fallback
    reason = _allow_limited_conn.get()
    if reason is not None:
        return True, reason

    return False, None


def get_no_dial(context: dict[str, Any] | None = None) -> tuple[bool, str | None]:
    """
    Check if the no dial option is set in the context.

    Args:
        context: Optional context dictionary

    Returns:
        Tuple of (no_dial, reason) where no_dial is True if dialing is disabled,
        and reason is the reason string if provided

    """
    if context and "no_dial" in context:
        return True, context["no_dial"]

    # Check context variable as fallback
    reason = _no_dial.get()
    if reason is not None:
        return True, reason

    return False, None


def get_force_direct_dial(
    context: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    """
    Check if the force direct dial option is set in the context.

    Args:
        context: Optional context dictionary

    Returns:
        Tuple of (force_direct, reason) where force_direct is True if direct dialing
        is forced, and reason is the reason string if provided

    """
    if context and "force_direct_dial" in context:
        return True, context["force_direct_dial"]

    # Check context variable as fallback
    reason = _force_direct_dial.get()
    if reason is not None:
        return True, reason

    return False, None


def set_allow_limited_conn(reason: str) -> None:
    """
    Set the allow limited connection option in the current context.

    Args:
        reason: Reason for allowing limited connections

    """
    _allow_limited_conn.set(reason)


def set_no_dial(reason: str) -> None:
    """
    Set the no dial option in the current context.

    Args:
        reason: Reason for not dialing

    """
    _no_dial.set(reason)


def set_force_direct_dial(reason: str) -> None:
    """
    Set the force direct dial option in the current context.

    Args:
        reason: Reason for forcing direct dial

    """
    _force_direct_dial.set(reason)


def clear_context() -> None:
    """Clear all context variables."""
    _allow_limited_conn.set(None)
    _no_dial.set(None)
    _force_direct_dial.set(None)
