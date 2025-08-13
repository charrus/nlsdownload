#!/usr/bin/env python

import asyncio
from io import BytesIO

import httpx
from PIL import Image


async def main():
    imageurl = "https://map-view.nls.uk/iiif/2/10234%2F102345876/info.json"

    async with httpx.AsyncClient() as client:
        r = await client.get(imageurl)
        if r.status_code != 200:
            print(f"Error fetching image info: {r.status_code}")
            return
        image_data = r.json()
        base_url = image_data.get("id", image_data.get("@id"))
        path = base_url.split("/")[-1]
        tile_width = image_data["tiles"][0]["width"]
        tile_height = image_data["tiles"][0]["height"]
        width = image_data["width"]
        height = image_data["height"]
        # Pick the smallest scale factor(usually 1x)
        # This is to ensure we get the highest resolution tiles available
        scale_factor = min(image_data["tiles"][0]["scaleFactors"])
        scale = image_data["tiles"][0]["scaleFactors"].index(scale_factor)
        montage = Image.new("RGB", (width, height))
        for x in range(0, width, tile_width):
            for y in range(0, height, tile_height):
                # Calculate the tile dimensions, ensuring we don't exceed the
                # image bounds.
                # This is to handle the last row/column which may not be a
                # full tile.
                this_tile_width = min(tile_width, width - x)
                this_tile_height = min(tile_height, height - y)
                tile_url = (
                    f"{base_url}/{x},{y},{this_tile_width},"
                    f"{this_tile_height}/{this_tile_width},{this_tile_height}/"
                    f"{scale}/default.jpg"
                )
                print(f"Fetching tile: {tile_url}")
                tile_response = await client.get(tile_url)
                # tile_response = httpx.get(tile_url)
                if tile_response.status_code == 200:
                    print(f"Tile fetched successfully: {tile_url}")
                    tile_image = Image.open(BytesIO(tile_response.content))
                    montage.paste(tile_image, (x, y))
                else:
                    print(
                        f"Failed to fetch tile: {tile_url} - "
                        f"{tile_response.status_code}"
                    )

    output_path = f"{path}.jpg"
    montage.save(output_path)
    print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
