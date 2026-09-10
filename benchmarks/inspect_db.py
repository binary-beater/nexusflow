import asyncio

from sqlalchemy import select

from nexusflow.config.settings import NexusFlowSettings
from nexusflow.persistence.engine import create_engine_and_session_factory
from nexusflow.persistence.orm import (
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)


async def main():
    settings = NexusFlowSettings()
    _, factory = create_engine_and_session_factory(settings.database.url)
    async with factory() as session:
        tasks = (await session.scalars(select(TaskExecutionRecord))).all()
        print(f"Total tasks: {len(tasks)}")
        for t in tasks:
            wf = await session.get(WorkflowExecutionRecord, t.workflow_execution_id)
            print(f"Task {t.task_definition_id}, state={t.state}, wf_id={t.workflow_execution_id}, wf_state={wf.state if wf else None}, wf_def_id={wf.definition_id if wf else None}")
            if wf:
                defn = await session.get(RegisteredDefinitionRecord, wf.definition_id)
                print(f"  Defn {defn.definition_id if defn else None}: {defn.workflow_name if defn else None}, tasks={list(defn.validated_iws.get('tasks', {}).keys()) if defn else None}")

if __name__ == "__main__":
    asyncio.run(main())
