import aiohttp


def make_session(**kwargs) -> aiohttp.ClientSession:
    """Create aiohttp session using ThreadedResolver (works without aiodns)."""
    import ssl as _ssl
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    connector = aiohttp.TCPConnector(
        resolver=aiohttp.resolver.ThreadedResolver(),
        ssl=ctx,
    )
    return aiohttp.ClientSession(connector=connector, **kwargs)
