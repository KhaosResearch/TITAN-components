from drama.manager import TaskManager
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute


task_excel2json = TaskRequest(
    name="EXCEL2JSON",
    module="drama_enbic2lab.catalog.soil.Excel2Json",
    params={"author": "Khaos developer", "group": "Khaos", "project": "Soil data"},
    inputs={"TempFile": "LOADZIP.TempFile"},
)

task_load = TaskRequest(
    name="LOADZIP",
    module="drama.core.catalog.load.ImportFile",
    params={"url": "http://192.168.213.23:8090/soil/input_soil.zip"},
    inputs={},
)

workflow_request = WorkflowRequest(tasks=[task_load, task_excel2json])

workflow = execute(workflow_request)
print(workflow)

# gets results
print(TaskManager().find({"parent": workflow.id}))
