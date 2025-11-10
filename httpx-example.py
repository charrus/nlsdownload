import asyncio
import httpx

async def main():
    async with httpx.AsyncClient() as client:
        response = await client.get("https://map-view.nls.uk/iiif/2/10234%2F102345876/info.json")
        print(response)

asyncio.run(main())
