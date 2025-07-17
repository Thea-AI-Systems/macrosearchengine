from datasets.Inflation import CPI_YoY_IN
import asyncio

async def main():
    await CPI_YoY_IN.update()


if __name__ == "__main__":    
    asyncio.run(main())