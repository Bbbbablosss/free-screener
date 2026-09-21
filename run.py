import os
import uvicorn

if __name__ == "__main__":
    # loop="asyncio": force the stdlib event loop instead of uvloop.
    # uvloop's native SSL layer (uvloop.loop.SSLProtocol) core-dumps with
    # SIGABRT under the heavy wss:// reconnect churn from a dozen exchanges,
    # restarting the process every ~90s and trapping it in perpetual warm-up.
    # The stdlib asyncio loop (epoll) is what the app ran on stably under
    # Windows and does not have this crash.
    uvicorn.run("backend.main:app", host=os.environ.get("WEB_HOST", "127.0.0.1"),
                port=int(os.environ.get("WEB_PORT", "8100")), reload=False,
                log_level="info", loop="asyncio")
