#!/usr/bin/env python
"""IIF Tile Downloader."""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiofiles
import httpx
from PIL import Image

QUEUE_SIZE = 16


async def consumer(queue: asyncio.Queue, client: httpx.AsyncClient):
    """Process tiles from the queue."""
    while True:
        tile = await queue.get()
        if not tile["file"].exists():
            for retry in range(2):
                r = await client.get(tile.get("url"))
                if r.status_code == 200:
                    if retry:
                        sys.stderr.write("*")
                    else:
                        sys.stderr.write("#")
                async with aiofiles.open(tile["file"], mode="wb") as img:
                    await img.write(r.content)
                break  # Exit from retry loop
            else:
                sys.stderr.write("!")
                await asyncio.sleep(5)

        sys.stderr.flush()
        queue.task_done()


def tile_url(
    base_url: str,
    scale: int,
    img_type: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> str:
    """Generate the URL for a tile image."""
    return (
        f"{base_url}/"
        f"{x},{y},{width},{height}/"
        f"{width},{height}/{scale}/default.{img_type}"
    )


def tile_filename(path: str, img_type: str, x: int, y: int) -> str:
    """Generate the filename for a tile image."""
    return f"{path}_{x}_{y}.{img_type}"


async def main(file: str, output_path: str):
    """Download IIF tiles and create a montage image."""
    queue = asyncio.Queue()

    tmpdir = Path("tiles")
    tmpdir.mkdir(exist_ok=True)

    async with httpx.AsyncClient(http2=True) as client:
        async with aiofiles.open(file, mode="r") as image_data_file:
            image_data_contents = await image_data_file.read()

            image_data = json.loads(image_data_contents)
            image_data = image_data["data"]["result"][0]
            base_url = image_data["overlays"][0]["overlay"]["url"]
            path = image_data["slug"]
            startx = 261808 - 9
            # startx = 1047234 - 19
            # endx = 1047385
            endx = startx + 55
            # starty = 697468 + 10
            starty = 174377 - 10
            endy = starty + 38
            image_data["width"] = (endx - startx) * 256
            image_data["height"] = (endy - starty) * 256
            tile_width = 256
            tile_height = 256
            scale = image_data["overlays"][0]["overlay"]["max_zoom"]
            img_type = base_url.split(".")[-1]
            tiles = []
            tasks = []

            for _ in range(QUEUE_SIZE):
                tasks.append(asyncio.create_task(consumer(queue, client)))

            for x in range(startx, endx):
                for y in range(starty, endy):
                    tile = {
                        "x": tile_width * (x - startx),
                        "y": tile_height * (y - starty),
                        "z": scale,
                    }
                    tile["file"] = Path(tmpdir, tile_filename(path, img_type, x, y))
                    tile["url"] = (
                        base_url.replace("{x}", str(x))
                        .replace("{y}", str(y))
                        .replace("{z}", str(scale))
                    )
                    tiles.append(tile)
                    queue.put_nowait(tile)

            await queue.join()

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        print()
        print(f"Creating montage {output_path}")
        montage = Image.new("RGB", (image_data["width"], image_data["height"]))
        for tile in tiles:
            try:
                with Image.open(tile["file"]) as im:
                    montage.paste(im, (tile["x"], tile["y"]))
            except Exception as e:
                print(f"Error processing {tile['file']}: {e}")
            finally:
                continue
        montage.save(output_path)
        print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(prog="iif2", usage="%(prog)s [options]")
    parser.add_argument(
        "--xyz",
        help=("XYZ Json"),
        default=("1940s.json"),
    )
    parser.add_argument("--output", help="Output filename")
    args = parser.parse_args()
    # Use provided manifest URL or default to example
    xyzfile = args.xyz
    if args.output:
        output_path = args.output
    else:
        output_path = "output.jpg"
    asyncio.run(main(xyzfile, output_path))
    total_slept_for = time.monotonic() - started_at
    print(f"{QUEUE_SIZE} workers took {total_slept_for:.2f} seconds")
