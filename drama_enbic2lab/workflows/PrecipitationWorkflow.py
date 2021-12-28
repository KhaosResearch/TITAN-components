from drama.manager import TaskManager
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute


task_load = TaskRequest(
    name="LOADDATA",
    module="drama.core.catalog.load.ImportFile",
    params={"url": "http://0.0.0.0:9000/temperature/AEMETPrecipitationTest.xlsx"},
    inputs={},
)

task_matrix_transformation = TaskRequest(
    name="PRECIPITATIONMATRIXTRANSFORMATION",
    module="drama_enbic2lab.catalog.water.PrecipitationMatrixTransformation",
    params={},
    inputs={"TempFile": "LOADDATA.TempFile"},
)

task_statistical = TaskRequest(
    name="DATAEXTRACTION",
    module="drama_enbic2lab.catalog.water.DataExtraction",
    params={},
    inputs={"SimpleTabularDataset": "PRECIPITATIONMATRIXTRANSFORMATION.SimpleTabularDataset"},
)

task_completition = TaskRequest(
    name="PRECIPITATIONSERIESCOMPLETITION",
    module="drama_enbic2lab.catalog.water.PrecipitationSeriesCompletition",
    params={
        "start_date": "1974-10-01",
        "end_date": "2018-09-30",
        "target_station": "GALAROZA",
        "analysis_stations": [
            "JABUGO",
            "CORTEGANA",
            "ARACENA",
            "ALAJAR",
        ],
        "priorize": "r2",
        "tests": ["pettit", "shnt", "buishand"],
    },
    inputs={"SimpleTabularDataset": "PRECIPITATIONMATRIXTRANSFORMATION.SimpleTabularDataset"},
)

workflow_request = WorkflowRequest(tasks=[task_load, task_matrix_transformation, task_statistical, task_completition])

workflow = execute(workflow_request)
print(workflow)

# gets results
print(TaskManager().find({"parent": workflow.id}))
