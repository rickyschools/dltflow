"""
dltflow.models.py
-------------------
This module contains the Pydantic models for the DLTFlow configuration file.

"""

from typing import Mapping

from pydantic import BaseModel, Field


class ProjectInfo(BaseModel):
    """
    ProjectInfo
    -----------
    Pydantic model for the project information.
    """

    name: str = Field(..., description="Name of the project")


class PathName(BaseModel):
    """
    PathName
    --------
    Pydantic model for the path information.
    """

    __pydantic_config__ = {"extra": "allow"}
    name: str = Field(..., description="Name of the path")


class ProjectElements(BaseModel):
    """
    ProjectElements
    ----------------
    Pydantic model for the project elements.
    """

    code: PathName = Field(
        default_factory=lambda: PathName(name="src", launch_method="launch"),
        description="Code of the project",
    )
    conf: PathName = Field(
        default_factory=lambda: PathName(name="conf"),
        description="Configuration of the project",
    )
    workflows: PathName = Field(
        default_factory=lambda: PathName(name="workflows"),
        description="Workflow of the project",
    )


class Workspace(BaseModel):
    """
    Workspace
    ----------
    Pydantic model for the workspace.
    """

    host: str = Field(..., description="Host of the workspace")
    root_path: str = Field(..., description="Root path of the workspace")
    artifact_path: str = Field(..., description="Artifact path of the workspace")


class Environment(BaseModel):
    """
    Environment
    ------------
    Pydantic model for the environment.
    """

    mode: str = Field(default="development")
    default: bool = Field(default=False)
    workspace: Workspace


class ProjectConfig(BaseModel):
    """
    ProjectConfig
    ---------------
    Pydantic model for the project configuration.
    """

    dltflow: ProjectInfo
    include: ProjectElements
    targets: Mapping[str, Environment]


# if __name__ == "__main__":
#     config = ProjectConfig(
#         dltflow=ProjectInfo(name='my_project'),
#         include=ProjectElements(),
#         targets={
#             'dev': Environment(
#                 mode='development',
#                 default='true',
#                 workspace=Workspace(
#                     host='databricks',
#                     root_path='/Users/{username}/cli_experiments/{project_name}'
#                 )
#             ),
#             'prod': Environment(
#                 mode='production',
#                 workspace=Workspace(
#                     host='databricks',
#                     root_path='/Users/{username}/cli_experiments/{project_name}'
#                 )
#             )
#         }
#     )
#     print(config.model_dump_yaml())
