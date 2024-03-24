from .connection import Connection
from .tcpfull import ConnectionTcpFull
from .tcpintermediate import ConnectionTcpIntermediate
from .tcpabridged import ConnectionTcpAbridged
from .tcpobfuscated import ConnectionTcpObfuscated
from .tcpmtproxy import (
    TcpMTProxy,
    ConnectionTcpMTProxyAbridged,
    ConnectionTcpMTProxyIntermediate,
    ConnectionTcpMTProxyRandomizedIntermediate
)
from .http import ConnectionHttp


def get_connection_class(connection_type: str = None):
    if connection_type == "tcp":
        return ConnectionTcpFull
    elif connection_type == "tcp_intermediate":
        return ConnectionTcpIntermediate
    elif connection_type == "tcp_abridged":
        return ConnectionTcpAbridged
    elif connection_type == "tcp_obfuscated":
        return ConnectionTcpObfuscated
    elif connection_type == "tcp_mtproxy_abridged":
        return ConnectionTcpMTProxyAbridged
    elif connection_type == "tcp_mtproxy_intermediate":
        return ConnectionTcpMTProxyIntermediate
    elif connection_type == "tcp_mtproxy":
        return ConnectionTcpMTProxyRandomizedIntermediate
    elif connection_type == "http":
        return ConnectionHttp
    else:
        return ConnectionTcpFull
