import asyncio
from prefect.deployments import Deployment
from prefect.logging import get_logger

# Import the flows to be deployed
from flows import (
    nhs_bed_occupancy_pipeline_flow,
    get_bed_occupancy_data_flow
)

logger = get_logger(__name__)

async def create_deployments():
    """
    Creates and applies all necessary deployments to the Prefect server.
    """
    logger.info("Starting deployment creation process...")

    # Wait for the Prefect server to be fully available
    logger.info("Waiting 30 seconds for Prefect server to initialize...")
    await asyncio.sleep(30)

    # Build the deployment objects from the imported flows
    try:
        logger.info("Building deployments...")
        nhs_deployment = await Deployment.build_from_flow(
            flow=nhs_bed_occupancy_pipeline_flow,
            name="nhs-bed-occupancy-pipeline-deployment",
            # --- THIS IS THE FIX ---
            # Use work_pool_name to explicitly match the worker's pool.
            work_pool_name="default"
        )

        get_data_deployment = await Deployment.build_from_flow(
            flow=get_bed_occupancy_data_flow,
            name="get-bed-occupancy-data-deployment",
            # --- THIS IS THE FIX ---
            # Use work_pool_name to explicitly match the worker's pool.
            work_pool_name="default"
        )

        # Apply the deployments to the server
        logger.info("Applying deployments to the server...")
        await nhs_deployment.apply()
        await get_data_deployment.apply()
        
        logger.info("All deployments created successfully. The script will now exit.")

    except Exception as e:
        logger.error(f"FATAL: Failed to create or apply deployments. Error: {e}")
        # Exit with an error code if deployments fail
        exit(1)


if __name__ == "__main__":
    # Run the main asynchronous function to create the deployments
    asyncio.run(create_deployments())
