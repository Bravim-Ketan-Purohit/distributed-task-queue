import asyncio, time, json, os, httpx, asyncpg, sys

API="http://localhost:7201"; DUR=int(sys.argv[1]) if len(sys.argv)>1 else 300
DSN=os.environ.get("DATABASE_URL_SYNC","postgresql://dtq:dtq@localhost:7203/dtq")

async def count(conn):
    return await conn.fetchval("SELECT count(*) FROM tasks WHERE queue='bench-engine' AND state='succeeded'")

async def producer(client, stop, n):
    i=0
    while not stop.is_set():
        try:
            await client.post(f"{API}/v1/tasks", json={"queue":"bench-engine","task_name":"noop",
                "payload":{"i":i},"dedup_key":f"bench-{n}-{i}"}, timeout=10)
            i+=1
        except Exception: await asyncio.sleep(0.05)
    return i

async def main():
    conn = await asyncpg.connect(DSN)
    start_done = await count(conn)
    stop = asyncio.Event()
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=200)) as client:
        prods=[asyncio.create_task(producer(client,stop,k)) for k in range(24)]
        t0=time.time(); last=start_done
        for s in range(DUR//15):
            await asyncio.sleep(15)
            c=await count(conn); el=time.time()-t0
            print(f"[{int(el)}s] succeeded={c-start_done} rate={(c-start_done)/el:.0f}/s window={(c-last)/15:.0f}/s", flush=True)
            last=c
        stop.set(); enq=sum(await asyncio.gather(*prods))
    el=time.time()-t0; done=(await count(conn))-start_done
    res={"path":"engine (control plane -> redis -> worker -> postgres commit)","duration_seconds":round(el,1),
         "enqueued":enq,"completed":done,"complete_rate_per_sec":round(done/el,1),
         "events_per_day":int(done/el*86400),"workers":4,"concurrency_per_worker":32}
    print(json.dumps(res,indent=2))
    open("bench/results/engine-path.json","w").write(json.dumps(res,indent=2))
    await conn.close()
asyncio.run(main())
