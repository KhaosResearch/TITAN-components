from drama.manager import TaskManager
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute


task_spss_to_csv = TaskRequest(
    name="SPSSTOCSV",
    module="drama_enbic2lab.catalog.soil.SpssToCSV",
    params={"drop_index": True},
    inputs={"TempFile": "LOADSPSS.TempFile"},
)

task_load = TaskRequest(
    name="LOADSPSS",
    module="drama.core.catalog.load.ImportFile",
    params={"url": "http://192.168.213.3:9000/soil/ExampleData.sav"},
    inputs={},
)

task_normalization = TaskRequest(
    name="DATANORMALIZATION",
    module="drama_enbic2lab.catalog.soil.DataNormalization",
    params={},
    inputs={"SimpleTabularDataset": "SPSSTOCSV.SimpleTabularDataset"},
)

task_pca = TaskRequest(
    name="PCA",
    module="drama_enbic2lab.catalog.soil.PCA",
    params={
        "variance_explained": 75,
        "whiten": True,
        "number_components": 0,
    },
    inputs={"SimpleTabularDataset": "DATANORMALIZATION.SimpleTabularDataset"},
)

workflow_request = WorkflowRequest(tasks=[task_load, task_spss_to_csv, task_normalization, task_pca])

workflow = execute(workflow_request)
print(workflow)

# gets results
print(TaskManager().find({"parent": workflow.id}))
