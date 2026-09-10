import asyncio

from sqlalchemy import text

from nexusflow.config.settings import NexusFlowSettings
from nexusflow.persistence.engine import create_engine_and_session_factory


async def clean():
    settings = NexusFlowSettings()
    _, factory = create_engine_and_session_factory(settings.database.url)
    async with factory() as session:
        # Clean test executions
        await session.execute(text("DELETE FROM history_entries;"))
        await session.execute(text("DELETE FROM execution_attempts;"))
        await session.execute(text("DELETE FROM task_executions;"))
        await session.execute(text("DELETE FROM workflow_executions;"))
        await session.execute(text("DELETE FROM registered_definitions WHERE workflow_name LIKE 'test_%' OR workflow_name LIKE 'scen_%' OR workflow_name LIKE 'race_%';"))
        await session.commit()
        print("Cleaned test executions and definitions successfully.")

if __name__ == "__main__":
    asyncio.run(clean())
