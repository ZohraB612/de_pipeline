import asyncio
from prefect.deployments import Deployment
from prefect.logging import get_logger

from flows import (
    nhs_bed_occupancy_pipeline_flow,
    get_bed_occupancy_data_flow
)

logger = get_logger(__name__)

async def create_deployments():
    """Create and apply deployments to the Prefect server."""
    logger.info("Starting deployment creation...")
    await asyncio.sleep(30)  # Wait for Prefect server

    try:
        logger.info("Building deployments...")
        nhs_deployment = await Deployment.build_from_flow(
            flow=nhs_bed_occupancy_pipeline_flow,
            name="Data Pipeline Flow",
            work_pool_name="default"
        )

        get_data_deployment = await Deployment.build_from_flow(
            flow=get_bed_occupancy_data_flow,
            name="get-bed-occupancy-data-deployment",
            work_pool_name="default"
        )

        logger.info("Applying deployments...")
        await nhs_deployment.apply()
        await get_data_deployment.apply()
        
        logger.info("Deployments created successfully.")

    except Exception as e:
        logger.error(f"Failed to create deployments: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(create_deployments())
