"""Strict external boundary DTOs for YAML definition ingestion (LLD-03 Section 6.1)."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
    )


class LiteralBindingDTO(StrictBaseModel):
    type: Literal["literal"]
    value: object


class WorkflowInputBindingDTO(StrictBaseModel):
    type: Literal["workflow_input"]


class TaskOutputBindingDTO(StrictBaseModel):
    type: Literal["task_output"]
    task: str


InputBindingDTO = Annotated[
    LiteralBindingDTO | WorkflowInputBindingDTO | TaskOutputBindingDTO,
    Field(discriminator="type"),
]


class WorkflowTaskOutputBindingDTO(StrictBaseModel):
    type: Literal["task_output"]
    task: str


WorkflowOutputBindingDTO = WorkflowTaskOutputBindingDTO


class TaskDefinitionDTO(StrictBaseModel):
    activity_type: str
    dependencies: list[str] = Field(default_factory=list)
    input_bindings: dict[str, InputBindingDTO] = Field(default_factory=dict)
    max_attempts: int


class WorkflowDefinitionDTO(StrictBaseModel):
    workflow_name: str
    tasks: dict[str, TaskDefinitionDTO]
    output_bindings: dict[str, WorkflowOutputBindingDTO] = Field(default_factory=dict)
