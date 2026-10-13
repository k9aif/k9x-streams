# SPDX-License-Identifier: Apache-2.0
# K9X Streams
"""K9X Streams service: one process runs the generator, the Kafka consumer, the live
state store, the MCP server (/mcp), the agent, and the live page (/).

    uvicorn streams.app:app --port 8780
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from streams import config
from streams.consumer import run_consumer
from streams.generator import VENDORS, generator
from streams.hub import hub
from streams.mcp_server import mcp
from streams.state import store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("k9x_streams")
WEB = Path(__file__).resolve().parent.parent / "web"
_agent_busy = asyncio.Lock()


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    hub.loop = asyncio.get_running_loop()
    consumer_task = None
    try:
        await generator.start_producer()
        consumer_task = asyncio.create_task(run_consumer())
    except Exception as exc:
        log.warning("Kafka not reachable at %s: %s", config.KAFKA_BROKER, exc)
    async with mcp.session_manager.run():
        yield
    generator.stop()
    if consumer_task:
        consumer_task.cancel()
    await generator.stop_producer()


app = FastAPI(title="K9X Streams", lifespan=lifespan)


@app.get("/")
def landing():
    return FileResponse(WEB / "landing.html", headers={"Cache-Control": "no-cache"})


@app.get("/demo")
def demo():
    return FileResponse(WEB / "demo.html", headers={"Cache-Control": "no-cache"})


@app.get("/api/health")
def health():
    return {"status": "ok", "broker": config.KAFKA_BROKER, "topics": config.TOPICS}


@app.get("/api/state")
def state():
    return {**store.snapshot(), "generator": {"running": generator.running, "rate": generator.rate},
            "topics": config.TOPICS, "max_age_seconds": config.MAX_AGE_SECONDS}


@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue()
    hub.queues.add(q)

    async def gen():
        try:
            yield "retry: 3000\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            hub.queues.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class GenReq(BaseModel):
    rate: float = 1.0


@app.post("/api/generator/start")
async def gen_start(req: Optional[GenReq] = None):
    generator.start((req or GenReq()).rate)
    hub.publish("generator", running=True, rate=generator.rate)
    return {"running": True, "rate": generator.rate}


@app.post("/api/generator/stop")
async def gen_stop():
    generator.stop()
    hub.publish("generator", running=False, rate=generator.rate)
    return {"running": False}


SCENARIOS = {"release", "duplicate_payment", "vendor_flagged", "poisoned_note"}


@app.post("/api/scenario/{name}")
async def scenario(name: str):
    """Approve a fresh invoice, start the agent on it, and (except 'release') inject the
    scenario's event into the stream while the agent is reviewing."""
    if name not in SCENARIOS:
        raise HTTPException(404, f"unknown scenario; one of {sorted(SCENARIOS)}")
    if _agent_busy.locked():
        raise HTTPException(409, "the agent is already reviewing an invoice")
    if generator.producer is None:
        raise HTTPException(503, f"Kafka not reachable at {config.KAFKA_BROKER}")
    inv = await generator.approved_invoice()
    run = uuid.uuid4().hex[:8]
    hub.publish("scenario", name=name, run=run, invoice=inv)
    asyncio.create_task(_run_scenario(name, run, inv))
    return {"run": run, "invoice": inv}


async def _run_scenario(name: str, run: str, inv: Dict[str, Any]) -> None:
    from streams.agent import PaymentReleaseAgent
    async with _agent_busy:
        await asyncio.sleep(0.8)   # let the consumer fold the new invoice in
        agent = PaymentReleaseAgent(on_step=lambda **s: hub.publish("agent", **s))
        loop = asyncio.get_running_loop()
        task = loop.run_in_executor(None, agent.execute,
                                    {"invoice_id": inv["invoice_id"], "run": run, "review_seconds": 4})
        await asyncio.sleep(2.0)   # mid-review: the world changes
        if name == "duplicate_payment":
            hub.publish("inject", run=run, text=f"payment.posted for {inv['invoice_id']} (another system paid it)")
            await generator.post_payment(inv["invoice_id"], inv["amount"])
        elif name == "vendor_flagged":
            hub.publish("inject", run=run, text=f"vendor.flagged: {inv['vendor_name']} on a sanctions list")
            await generator.flag_vendor(inv["vendor_id"], inv["vendor_name"])
        elif name == "poisoned_note":
            hub.publish("inject", run=run, text=f"vendor.note for {inv['vendor_name']} carrying hidden instructions")
            await generator.poisoned_note(inv["vendor_id"], inv["vendor_name"])
        try:
            await task
        except Exception as exc:
            log.exception("agent run %s failed", run)
            hub.publish("agent", run=run, step="decision", decision="ERROR", reason=str(exc)[:300],
                        invoice_id=inv["invoice_id"])


app.mount("/images", StaticFiles(directory=str(WEB / "images")), name="images")

# MCP endpoint at /mcp (FastMCP's streamable-HTTP app), mounted last so the routes above win.
app.mount("/", mcp.streamable_http_app())
